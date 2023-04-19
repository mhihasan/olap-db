import itertools
from concurrent.futures import FIRST_COMPLETED, wait, ProcessPoolExecutor


def run_concurrent_process(fn, input_params, *, max_concurrency):
    input_params_iter = iter(input_params)
    total_tasks = len(input_params)
    total_completed_tasks = 0

    with ProcessPoolExecutor(max_workers=max_concurrency) as executor:
        futures = {executor.submit(fn, param): param for param in itertools.islice(input_params_iter, max_concurrency)}

        while futures:
            finished_tasks, _ = wait(futures, return_when=FIRST_COMPLETED)

            for task in finished_tasks:
                param = futures.pop(task)
                print("Finished param", param)

            total_completed_tasks += len(finished_tasks)
            print(
                f"Completed tasks: {total_completed_tasks}/{total_tasks}, {round(total_completed_tasks * 100 / total_tasks, 2)}%"  # noqa
            )

            for param in itertools.islice(input_params_iter, len(finished_tasks)):
                futures[executor.submit(fn, param)] = param

        executor.shutdown()
