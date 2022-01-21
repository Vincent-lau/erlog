#!/usr/bin/env zsh

cd apps/erlog/bench/bench_program

# include general utilities
. ./utils.sh


for N in {200..200..50}
    do
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


        rebar3 compile
    done
