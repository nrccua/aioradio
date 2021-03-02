"""aioradio utils cache script."""

from asyncio import coroutine, create_task, sleep, to_thread
from typing import Any, Dict, List, Tuple


async def manage_async_tasks(items: List[Tuple[coroutine, str]], concurrency: int) -> Dict[str, Any]:
    """Manages a grouping of async tasks, keeping number of active tasks at the
    concurrency level by starting new tasks whenver one completes.

    Args:
        items (List[Dict[str, Any]]): List of tuples (coroutine, name)
        concurrency (int): max concurrency

    Returns:
        Dict[str, Any]: Dict with task name as the key, task result as the value
    """

    results = {}
    arr = [create_task(coro) if name is None else create_task(coro, name=name) for coro, name in items[:concurrency]]
    count = len(arr)
    num_of_items = len(items)
    while len(results) < num_of_items:
        await sleep(0.001)
        for index, task in enumerate(arr):
            if task.done():
                results[task.get_name()] = await task
                if count < num_of_items:
                    coro, name = items[count]
                    arr[index] = create_task(coro) if name is None else create_task(coro, name=name)
                    count += 1

    return results

async def manage_async_to_thread_tasks(func: Any, items: List[Dict[str, Any]], concurrency: int) -> Dict[str, Any]:
    """Manages a grouping of async.to_thread tasks, keeping number of active
    tasks at the concurrency level by starting new tasks whenver one completes.
    Only use with python3.9+.

    Args:
        func (Any): Function to run in threads
        items (List[Dict[str, Any]]): List of dict with kwargs for each task
        concurrency (int): max concurrency

    Returns:
        Dict[str, Any]: Dict with generi task name as the key, task result as the value
    """

    results = {}
    arr = [create_task(to_thread(func, **i)) for i in items[:concurrency]]
    count = len(arr)
    num_of_items = len(items)
    while len(results) < num_of_items:
        await sleep(0.001)
        for index, task in enumerate(arr):
            if task.done():
                results[task.get_name()] = await task
                if count < num_of_items:
                    arr[index] = create_task(to_thread(func, **items[count]))
                    count += 1

    return results
