<template>
  <section class="page" data-module="batch">
    <header class="page-head">
      <div>
        <h2>批量转换管理</h2>
        <p class="page-desc">维护批量转换任务的输入、执行约束与结果记录，支持失败分区隔离重放：只从失败分区继续，已成功输出不重复写入。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记批处理任务</button>
        <button class="btn" type="button" @click="exportRows">导出批量转换清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>分区进度</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td>
            <template v-if="row['分区总数']">
              成功 {{ row['成功分区数'] }} / 失败 {{ row['失败分区数'] }} / 未完成 {{ row['未完成分区数'] }}
              <div v-if="row['失败分区数']" class="error-text">
                最早失败分区：{{ earliestFailure(row) ?? '—' }}
              </div>
            </template>
            <template v-else>—</template>
          </td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button
              v-if="row['失败分区数'] > 0 && !row['重放中']"
              class="link"
              type="button"
              @click="replay(row)"
            >
              失败隔离重放
            </button>
            <span v-if="row['重放中']" class="error-text">重放运行中</span>
            <button class="link" type="button" @click="openDetail(row)">失败清单/重放记录</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 2" class="empty-state">暂无批量转换数据，可先登记批处理任务</td>
        </tr>
      </tbody>
    </table>

    <section v-if="selected" class="replay-panel">
      <header class="page-head">
        <div>
          <h3>{{ selected['任务编号'] }} 失败清单与重放台账</h3>
          <p class="page-desc">
            输入路径 {{ selected['输入路径'] }} ｜ 分区覆盖 {{ selected['分区覆盖'] }} ｜
            依赖上游 {{ selected['依赖上游'] }} ｜ 处理脚本 {{ selected['处理脚本'] }}
          </p>
        </div>
        <div class="page-actions">
          <button class="btn ghost" type="button" @click="selected = null">关闭明细</button>
        </div>
      </header>

      <h4>失败历史（每次失败的时间与分区都保留，同分区冲突以最早记录为准）</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th v-for="column in failureColumns" :key="column">{{ column }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in failureRows" :key="item['失败记录编号']">
            <td v-for="column in failureColumns" :key="column">{{ item[column] ?? '—' }}</td>
          </tr>
          <tr v-if="!failureRows.length">
            <td :colspan="failureColumns.length" class="empty-state">暂无失败记录</td>
          </tr>
        </tbody>
      </table>

      <h4>重放实例（同一任务并发重放只允许一个执行实例）</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th v-for="column in instanceColumns" :key="column">{{ column }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in instanceRows" :key="item['重放实例编号']">
            <td v-for="column in instanceColumns" :key="column">{{ formatCell(item, column) }}</td>
          </tr>
          <tr v-if="!instanceRows.length">
            <td :colspan="instanceColumns.length" class="empty-state">暂无重放实例</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条批量转换记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { request } from '@/api/client'

// 分区结果、失败历史、重放实例均为嵌套结构，这里采用宽松行类型。
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Row = Record<string, any>

const ENDPOINT = '/api/batch'
const columns = ["任务编号", "输入路径", "处理脚本", "输出路径", "分区覆盖", "依赖上游", "重试策略", "超时终止"]
const actions = ["提交调度", "启动任务", "终止任务", "归档结果"]
const failureColumns = ["失败记录编号", "分区", "失败时间", "状态", "处理脚本", "依赖上游", "失败原因", "冲突说明", "解决时间"]
const instanceColumns = ["重放实例编号", "起始分区", "结束分区", "计划分区", "成功分区", "失败分区", "状态", "提交时间", "结束时间"]
const stats = [
  { label: "批处理任务总数", value: 0 },
  { label: "运行中任务", value: 0 },
  { label: "已完成任务", value: 0 },
  { label: "失败待重放分区", value: 0 },
]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const selected = ref<Row | null>(null)
const failureRows = ref<Row[]>([])
const instanceRows = ref<Row[]>([])

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '批处理任务登记入口尚未接入审批流'
}

function earliestFailure(row: Row): number | null {
  // 失败分区冲突时以最早失败记录为准：分区结果里取状态为失败的最小区号。
  const results = row['分区结果'] as Record<string, Row> | undefined
  if (!results) return null
  const failed = Object.values(results)
    .filter((part) => part['状态'] === '失败')
    .map((part) => Number(part['分区']))
  return failed.length ? Math.min(...failed) : null
}

function formatCell(item: Row, column: string): string {
  const value = item[column]
  return Array.isArray(value) ? value.join(',') : String(value ?? '—')
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values: { action } }),
    })
    const payload = await response.json()
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.message ?? '批量转换动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批量转换操作失败'
  }
}

async function replay(row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/replay`, {
      method: 'POST',
      body: JSON.stringify({ values: {} }),
    })
    const payload = await response.json()
    if (!response.ok) {
      // 非法分区范围等 400 错误直接透出后端 detail，便于按脚本与上游定位。
      throw new Error(payload.detail ?? '失败隔离重放未生效')
    }
    errorMessage.value = payload.message ?? '重放已提交'
    await reload()
    if (selected.value && String(selected.value.id) === String(row.id)) {
      await openDetail(row)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '失败隔离重放失败'
  }
}

async function openDetail(row: Row) {
  errorMessage.value = ''
  selected.value = row
  try {
    const [failureResp, instanceResp] = await Promise.all([
      request(`${ENDPOINT}/${row.id}/failures`),
      request(`${ENDPOINT}/${row.id}/replay-instances`),
    ])
    if (!failureResp.ok || !instanceResp.ok) {
      throw new Error('失败清单或重放记录读取失败')
    }
    const failurePayload = await failureResp.json()
    const instancePayload = await instanceResp.json()
    failureRows.value = failurePayload.items ?? []
    instanceRows.value = instancePayload.items ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '失败明细读取失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('批处理任务列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    stats[0].value = rows.value.length
    stats[1].value = rows.value.filter((row) => row.status === '运行中').length
    stats[2].value = rows.value.filter((row) => row.status === '已完成').length
    stats[3].value = rows.value.reduce((sum, row) => sum + Number(row['失败分区数'] ?? 0), 0)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批量转换列表读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.replay-panel {
  margin-top: 24px;
  padding: 16px;
  border: 1px solid #d9e1ec;
  border-radius: 8px;
  background: #f8fafc;
}

.replay-panel h4 {
  margin: 16px 0 8px;
}
</style>
