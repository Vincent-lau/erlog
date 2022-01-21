-module(profiling).

-compile(export_all).

-define(PROG, "apps/erlog/bench/bench_program/tc_bench.dl").
-define(BENCH_DIR, "apps/erlog/bench/").

prof_single(fprof) ->
  fprof:trace(start, ?BENCH_DIR ++ "fprof.trace"),
  erlog_worker:run_program(single, ?PROG, "reachable"),
  fprof:trace(stop),
  fprof:profile(file, ?BENCH_DIR ++ "fprof.trace"),
  fprof:analyse({dest, ?BENCH_DIR ++ "fprof.analysis"});
prof_single(flame) ->
  eflame:apply(normal_with_children,
               ?BENCH_DIR ++ "stacks.out",
               erlog_worker,
               run_program,
               [single, ?PROG, "reachable"]),
  os:cmd("./_build/default/lib/eflame/stack_to_flame.sh < "
         ++ ?BENCH_DIR
         ++ "stacks.out"
         ++ " > "
         ++ ?BENCH_DIR
         ++ "flame.svg").
