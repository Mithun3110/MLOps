- Fixed run_sequential() to work properly with the Ray-decorated training function.

- Corrected run_parallel() to use .remote(), store object refs, and retrieve results with ray.get().

- Measured and displayed sequential vs. parallel execution times using identical model-training workloads.

- Added a comparison cell to check speedup and confirm both methods return matching results.