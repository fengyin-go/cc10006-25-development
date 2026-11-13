"""数据湖仓一体管理平台 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8000
健康检查：GET /api/health
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import ROUTERS
from app.store import store

app = FastAPI(title="数据湖仓一体管理平台", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

for module in ROUTERS:
    app.include_router(module.router)


@app.get("/api/health")
def health() -> dict[str, object]:
    """健康检查：确认服务已经监听、示例数据已经就绪。"""
    return {"ok": True, "app": settings.app_name, "modules": len(store.module_names())}


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。"""
    return store.overview()


@app.on_event("startup")
def sync_batch_failure_ledgers() -> None:
    """启动时用批处理失败清单对齐质量异常台账与清洗待处理队列。"""
    from app.services.batch import BatchService

    BatchService().sync_ledgers()


@app.post("/api/batch/sync-ledgers")
def sync_batch_failure_ledgers_manual() -> dict[str, object]:
    """手动触发失败清单跨模块同步：质量台账与清洗待处理队列按最新失败记录对齐。"""
    from app.services.batch import BatchService

    return {"ok": True, "synced": BatchService().sync_ledgers()}
