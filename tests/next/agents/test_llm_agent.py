from a2a.types.a2a_pb2 import TaskArtifactUpdateEvent

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.agents.llm_agent import LLMAgent
from new_choirworks.core.models.lite_llm import LiteLLM


async def test_text_response(llm_kwargs):
    llm = LiteLLM(**llm_kwargs)
    agent = LLMAgent(name="test", llm=llm)
    ctx = InvocationContext(messages=[{"role": "user", "content": "Hello"}])

    results = [e async for e in agent.run_async(ctx)]

    assert len(results) > 0
    artifacts = [e for e in results if isinstance(e, TaskArtifactUpdateEvent)]
    assert len(artifacts) > 0
    text = "".join(
        p.text for e in artifacts for p in e.artifact.parts if p.text
    )
    assert text
