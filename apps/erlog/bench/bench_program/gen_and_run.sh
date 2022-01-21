#!/usr/bin/env zsh

RES_FILE=

echo "=====================starting new iterations===================" >> apps/erlog/bench/results/timing_res.txt
for N in {50..100..50}
    do
        cd apps/erlog/bench/bench_program
        # include general utilities
        . ./utils.sh

        echo "current size of graph, i.e. value of N is $N"
        C=$N
        SIZE=$N
        E=`expr $C \* 10`    # each node has on average 10 neighbors

        # create fact files as needed
        #             | name | |entries| |       ranges        |
        gen_fact_file   base      $E    $C $C
        
        cp ./tc_program.dl ./tc_bench.dl
        ./to_atoms.py >> ./tc_bench.dl
        echo "graph generated into tc_bench.dl"

        echo "\n==============new timings===================" >> ../results/timing_res.txt
        echo "graph size $N" >> ../results/timing_res.txt

        cd ../../../../
        echo "going back to `pwd`"
        rebar3 compile

        echo "now running program"
        erl -pa "_build/default/lib/erlog/ebin" -pa "_build/default/lib/erlog/bench" -s timing start -s init stop -noinput

    done

echo "=========================done===============================" >> apps/erlog/bench/results/timing_res.tx