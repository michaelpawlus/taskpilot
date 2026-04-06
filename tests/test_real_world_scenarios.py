"""Real-world test scenarios based on r/FastAPI community pain points.

Each test maps to a specific problem post from r/FastAPI where developers
struggled with BackgroundTasks visibility, retries, or silent failures.
"""

import asyncio
import random

import pytest

import taskpilot
from taskpilot.middleware import TaskPilotMiddleware
from taskpilot.store import TaskStore


# ---------------------------------------------------------------------------
# Scenario 1: Email/notification pipeline — fire-and-forget with visibility
#
# Source: r/FastAPI "How do you know if your FastAPI BackgroundTasks actually
# ran?" — developers lose all visibility after add_task().
# ---------------------------------------------------------------------------


async def test_email_notification_visible_after_send(configured_db):
    """After sending an email in the background, I can query its status."""

    @taskpilot.track(store_result=True, tags=["email", "onboarding"])
    async def send_welcome_email(to: str, template: str):
        # Simulate SMTP send
        await asyncio.sleep(0.01)
        return {"to": to, "template": template, "message_id": "msg_abc123"}

    await send_welcome_email("newuser@example.com", "welcome_v2")

    store = TaskStore(str(configured_db))
    await store.connect()
    result = await store.list_tasks(function_name="send_welcome_email")
    assert result.total_matching == 1
    task = result.tasks[0]
    assert task.status == "succeeded"
    assert task.tags == ["email", "onboarding"]

    # The key differentiator: I can retrieve the result
    stored_result = await store.get_result(task.task_id)
    assert stored_result["message_id"] == "msg_abc123"
    assert stored_result["to"] == "newuser@example.com"
    await store.close()


async def test_email_failure_captured_not_silent(configured_db):
    """When SMTP fails, the error is recorded — not silently swallowed."""

    @taskpilot.track(tags=["email", "transactional"])
    async def send_receipt_email(order_id: str):
        raise ConnectionError("SMTP server refused connection on port 587")

    await send_receipt_email("order_9923")

    store = TaskStore(str(configured_db))
    await store.connect()
    result = await store.list_tasks(function_name="send_receipt_email")
    task = result.tasks[0]
    assert task.status == "failed"
    assert "SMTP server refused connection" in task.error_message

    # TaskSummary doesn't carry traceback — get full detail for that
    detail = await store.get_task_detail(task.task_id)
    assert detail.error_traceback is not None
    assert "ConnectionError" in detail.error_traceback
    await store.close()


# ---------------------------------------------------------------------------
# Scenario 2: Webhook delivery with retries
#
# Source: r/FastAPI "How are you actually managing background/async tasks in
# FastAPI in production?" — "retrying failed tasks" listed as top pain point.
# ---------------------------------------------------------------------------


async def test_webhook_delivery_retries_on_transient_failure(configured_db):
    """Webhook to a flaky endpoint retries and eventually succeeds."""
    attempt_count = 0

    @taskpilot.track(retries=3, backoff="none", store_result=True,
                     tags=["webhook", "stripe"])
    async def deliver_webhook(url: str, payload: dict):
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count < 3:
            raise ConnectionError(f"HTTP 503 from {url}")
        return {"status_code": 200, "attempt": attempt_count}

    await deliver_webhook("https://api.customer.com/hooks", {"event": "charge.succeeded"})

    store = TaskStore(str(configured_db))
    await store.connect()
    result = await store.list_tasks(function_name="deliver_webhook")
    task = result.tasks[0]
    assert task.status == "succeeded"
    assert task.retry_count == 2

    # Verify each retry attempt is recorded
    retries = await store.get_retries(task.task_id)
    assert len(retries) == 2
    assert "HTTP 503" in retries[0].error_message

    # Verify the successful result is stored
    stored = await store.get_result(task.task_id)
    assert stored["status_code"] == 200
    assert stored["attempt"] == 3
    await store.close()


async def test_webhook_delivery_exhausts_retries(configured_db):
    """Endpoint permanently down — retries exhaust, task marked dead."""

    @taskpilot.track(retries=2, backoff="none", tags=["webhook", "billing"])
    async def deliver_webhook(url: str, payload: dict):
        raise ConnectionError(f"Connection refused: {url}")

    await deliver_webhook("https://dead-endpoint.com/hook", {"event": "invoice.paid"})

    store = TaskStore(str(configured_db))
    await store.connect()
    result = await store.list_tasks(function_name="deliver_webhook")
    task = result.tasks[0]
    assert task.status == "dead"
    assert task.retry_count == 2

    retries = await store.get_retries(task.task_id)
    assert len(retries) == 2
    await store.close()


# ---------------------------------------------------------------------------
# Scenario 3: OCR document processing — CPU-bound with variable duration
#
# Source: r/FastAPI "FastAPI + OCR Pipeline — BackgroundTasks vs Celery/Redis?"
# — 33 upvotes, devs torn between BackgroundTasks simplicity and Celery
# observability.
# ---------------------------------------------------------------------------


async def test_ocr_processing_tracks_duration(configured_db):
    """OCR task records how long processing took."""

    @taskpilot.track(store_result=True, tags=["ocr", "document"])
    async def process_document(file_id: str, doc_type: str):
        # Simulate variable OCR processing time
        await asyncio.sleep(0.05)
        return {"file_id": file_id, "pages_extracted": 12, "confidence": 0.94}

    await process_document("doc_5521", "invoice")

    store = TaskStore(str(configured_db))
    await store.connect()
    result = await store.list_tasks(function_name="process_document")
    task = result.tasks[0]
    assert task.status == "succeeded"
    assert task.duration_ms is not None
    assert task.duration_ms >= 40  # At least ~50ms of simulated work

    stored = await store.get_result(task.task_id)
    assert stored["pages_extracted"] == 12
    await store.close()


async def test_ocr_batch_concurrent_processing(configured_db):
    """Batch of documents processed concurrently, all tracked individually."""

    @taskpilot.track(store_result=True, tags=["ocr", "batch"])
    async def process_document(file_id: str):
        await asyncio.sleep(0.01)
        return {"file_id": file_id, "status": "extracted"}

    file_ids = [f"doc_{i}" for i in range(8)]
    await asyncio.gather(*[process_document(fid) for fid in file_ids])

    store = TaskStore(str(configured_db))
    await store.connect()
    result = await store.list_tasks(function_name="process_document", limit=20)
    assert result.total_matching == 8
    succeeded = [t for t in result.tasks if t.status == "succeeded"]
    assert len(succeeded) == 8
    await store.close()


async def test_ocr_corrupted_file_fails_with_context(configured_db):
    """Corrupted PDF fails with actionable error, not generic traceback."""

    @taskpilot.track(retries=1, backoff="none", tags=["ocr"])
    async def process_document(file_id: str):
        raise ValueError(f"Cannot parse PDF {file_id}: header corrupted at byte 0")

    await process_document("doc_corrupt_001")

    store = TaskStore(str(configured_db))
    await store.connect()
    result = await store.list_tasks(function_name="process_document")
    task = result.tasks[0]
    assert task.status == "dead"
    assert "header corrupted" in task.error_message
    assert "doc_corrupt_001" in task.error_message
    await store.close()


# ---------------------------------------------------------------------------
# Scenario 4: ML/RAG pipeline — silent failure chain
#
# Source: r/FastAPI "FastAPI ML Service on Railway — BackgroundTasks +
# SentenceTransformer 502, Pinecone never getting indexed" — works locally,
# breaks in production with no error trail.
# ---------------------------------------------------------------------------


async def test_ml_embedding_failure_recorded(configured_db):
    """SentenceTransformer OOM is captured, not silently swallowed."""

    @taskpilot.track(retries=1, backoff="none", tags=["ml", "embedding"])
    async def generate_embeddings(doc_id: str, text: str):
        # Simulates a model loading failure in constrained env (Railway 512MB)
        raise MemoryError(
            f"Cannot allocate tensor for doc {doc_id}: "
            "SentenceTransformer requires 1.2GB, container limit 512MB"
        )

    await generate_embeddings("manual_042", "Installation instructions for...")

    store = TaskStore(str(configured_db))
    await store.connect()
    result = await store.list_tasks(function_name="generate_embeddings")
    task = result.tasks[0]
    assert task.status == "dead"
    assert "Cannot allocate tensor" in task.error_message
    assert "manual_042" in task.error_message
    await store.close()


async def test_ml_pipeline_partial_success_visible(configured_db):
    """In a multi-step pipeline, each step has its own tracked status."""

    @taskpilot.track(store_result=True, tags=["ml", "extract"])
    async def extract_text(doc_id: str):
        return {"doc_id": doc_id, "text": "extracted content", "pages": 5}

    @taskpilot.track(store_result=True, tags=["ml", "embed"])
    async def generate_embedding(doc_id: str, text: str):
        return {"doc_id": doc_id, "vector_dim": 384}

    @taskpilot.track(tags=["ml", "index"])
    async def index_to_pinecone(doc_id: str, vector: list):
        raise ConnectionError("Pinecone upsert timeout after 30s")

    # Step 1: succeeds
    await extract_text("doc_100")
    # Step 2: succeeds
    await generate_embedding("doc_100", "extracted content")
    # Step 3: fails
    await index_to_pinecone("doc_100", [0.1] * 384)

    store = TaskStore(str(configured_db))
    await store.connect()

    # Can query by tag to see which pipeline stage failed
    extract = await store.list_tasks(function_name="extract_text")
    assert extract.tasks[0].status == "succeeded"

    embed = await store.list_tasks(function_name="generate_embedding")
    assert embed.tasks[0].status == "succeeded"

    index = await store.list_tasks(function_name="index_to_pinecone")
    assert index.tasks[0].status == "failed"
    assert "Pinecone upsert timeout" in index.tasks[0].error_message
    await store.close()


# ---------------------------------------------------------------------------
# Scenario 5: Full middleware integration — FastAPI app with task visibility
#
# Source: all of the above — the middleware is how tasks get tracked in a
# real FastAPI app rather than standalone function calls.
# ---------------------------------------------------------------------------


@pytest.fixture
def email_app(tmp_db):
    """A realistic FastAPI app with email + webhook background tasks."""
    from fastapi import FastAPI, Request

    app = FastAPI()
    app.add_middleware(TaskPilotMiddleware, db_path=str(tmp_db))

    @taskpilot.track(retries=2, backoff="none", store_result=True,
                     tags=["email", "welcome"])
    async def send_welcome_email(user_id: str, email: str):
        await asyncio.sleep(0.01)
        return {"user_id": user_id, "sent": True}

    @taskpilot.track(tags=["webhook"])
    async def notify_crm(user_id: str, event: str):
        raise ConnectionError("CRM API down")

    @app.post("/signup")
    async def signup(request: Request):
        email_id = await request.state.taskpilot.run(
            send_welcome_email, user_id="usr_42", email="new@example.com"
        )
        return {"email_task_id": email_id}

    @app.post("/notify")
    async def notify(request: Request):
        task_id = await request.state.taskpilot.run(
            notify_crm, user_id="usr_42", event="signup"
        )
        return {"task_id": task_id}

    @app.get("/task/{task_id}")
    async def task_status(task_id: str, request: Request):
        return await request.state.taskpilot.status(task_id)

    return app


@pytest.fixture
async def email_client(email_app):
    from httpx import ASGITransport, AsyncClient
    transport = ASGITransport(app=email_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_signup_email_tracked_via_middleware(email_client, tmp_db):
    """Full flow: POST /signup triggers background email, status queryable."""
    resp = await email_client.post("/signup")
    assert resp.status_code == 200
    task_id = resp.json()["email_task_id"]

    # Query status through the app
    status_resp = await email_client.get(f"/task/{task_id}")
    assert status_resp.status_code == 200
    detail = status_resp.json()
    assert detail["status"] == "succeeded"
    assert detail["function_name"] == "send_welcome_email"


async def test_failed_webhook_visible_via_middleware(email_client, tmp_db):
    """CRM webhook fails — error is visible through task status endpoint."""
    resp = await email_client.post("/notify")
    task_id = resp.json()["task_id"]

    status_resp = await email_client.get(f"/task/{task_id}")
    detail = status_resp.json()
    assert detail["status"] == "failed"
    assert "CRM API down" in detail["error_message"]


# ---------------------------------------------------------------------------
# Scenario 6: Filtering and operational queries
#
# Source: r/FastAPI "needing visibility into what's running or failing" —
# operators need to answer "what failed in the last hour?" quickly.
# ---------------------------------------------------------------------------


async def test_filter_failed_tasks_across_functions(configured_db):
    """Operator can quickly find all failed tasks regardless of function."""

    @taskpilot.track(tags=["email"])
    async def send_email():
        raise RuntimeError("SMTP down")

    @taskpilot.track(tags=["webhook"])
    async def send_webhook():
        raise ConnectionError("timeout")

    @taskpilot.track(tags=["report"])
    async def generate_report():
        return "done"

    await send_email()
    await send_webhook()
    await generate_report()

    store = TaskStore(str(configured_db))
    await store.connect()

    # "Show me everything that failed"
    failed = await store.list_tasks(status="failed")
    assert failed.total_matching == 2
    names = {t.function_name for t in failed.tasks}
    assert names == {"send_email", "send_webhook"}

    # "Show me just email failures"
    email_failed = await store.list_tasks(status="failed", tags="email")
    assert email_failed.total_matching == 1
    assert email_failed.tasks[0].function_name == "send_email"

    summary = await store.get_status_summary()
    assert summary.by_status.get("succeeded", 0) == 1
    assert summary.by_status.get("failed", 0) == 2
    await store.close()
