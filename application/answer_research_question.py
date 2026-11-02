"""Interactive-research use case with one resource lifecycle."""

from __future__ import annotations


async def answer_research_question(
    question: str,
    *,
    enable_search: bool = True,
    enable_web: bool = False,
):
    from sovereign_hall.services.research_discussion import ResearchDiscussionSystem

    system = ResearchDiscussionSystem(
        enable_search=enable_search,
        enable_web=enable_web,
    )
    try:
        return await system.research(question)
    finally:
        await system.close()
