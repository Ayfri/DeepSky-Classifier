from fastapi import APIRouter, HTTPException, Request

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict[str, str]:
	return {"status": "ok"}


@router.get("/readiness")
def readiness(request: Request) -> dict[str, str]:
	"""Return 200 when the model is loaded, 503 otherwise.

	Kubernetes liveness probes use /health; readiness probes use this
	endpoint so the pod is only added to the load-balancer after the
	model artefact has been successfully deserialised.
	"""
	if getattr(request.app.state, "model", None) is None:
		raise HTTPException(status_code=503, detail="Model not ready")
	return {"status": "ready"}
