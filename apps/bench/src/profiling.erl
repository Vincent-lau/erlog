-module(profiling).

-compile(export_all).
-compile(nowarn_export_all).

-define(PROG, "apps/erlog/bench_program/tc_bench.dl").
-define(BENCH_DIR, "apps/bench/results/").

prof_single(fprof) ->
  fprof:trace(start, ?BENCH_DIR ++ "fprof.trace"),
  timing:start(),
  fprof:trace(stop),
  fprof:profile(file, ?BENCH_DIR ++ "fprof.trace"),
  fprof:analyse({dest, ?BENCH_DIR ++ "fprof.analysis"});
prof_single(flame) ->
  eflame:apply(normal_with_children,
               ?BENCH_DIR ++ "stacks.out",
               timing,
               start,
               []),
  os:cmd("./_build/default/lib/eflame/stack_to_flame.sh < "
         ++ ?BENCH_DIR
         ++ "stacks.out"
         ++ " > "
         ++ ?BENCH_DIR
         ++ "flame.svg");
prof_single(worker) ->
  eflame:apply(normal_with_children,
               ?BENCH_DIR ++ "worker.out",
               worker,
               start_working_sync,
               []),
  os:cmd("./_build/default/lib/eflame/stack_to_flame.sh < "
         ++ ?BENCH_DIR
         ++ "worker.out"
         ++ " > "
         ++ ?BENCH_DIR
         ++ "worker_flame_persist.svg");
prof_single(fprof_worker) ->
  fprof:trace(start, ?BENCH_DIR ++ "worker_opt.trace"),
  worker:start_working_sync(),
  fprof:trace(stop),
  fprof:profile(file, ?BENCH_DIR ++ "worker_opt.trace"),
  fprof:analyse({dest, ?BENCH_DIR ++ "worker_opt.analysis"}),
  os:cmd("erlgrind apps/bench/results/worker_opt.analysis apps/bench/results/worker_opt.cgrind").
