# Erlog: A distributed datalog engine

## Build

To build this engine, install Erlang/OTP (>24.0) and `rebar3` and then run

```bash
 rebar3 compile
```

## Run

For the syntax of a datalog program, you can check out a few examples in apps/erlog/doc/examples/.
Once you have a datalog program, change the field in `erlog.app.src` which says
`{mod, {erlog_app, ["path/to/file"]}}` to point to your datalog program. You can
also modify the `num_tasks` env variable to control how many tasks you want to split
the original input into.

Then open up a few shells, and use one of them as the coordinator, and the rest of 
workers. For example, you can do

```bash
# shell 1
rebar3 shell --name 'coor@127.0.0.1'

# shell 2
rebar3 shell --name 'worker1@127.0.0.1'

# shell 3
rebar3 shell --name 'worker2@127.0.0.1'

```

This is setup the cluster and once these Erlang nodes are up, run `erlog_srv:start_working()` in the coordinator node and the engine should now start to computing your program!

## Tests

To test the engine, use Erlang's Eunit and Common test framework

```bash 
rebar3 do eunit, ct
```
