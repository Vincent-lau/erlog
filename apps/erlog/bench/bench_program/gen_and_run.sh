#!/usr/bin/env zsh

# create a fresh result file
RES_FILE=apps/erlog/bench/results/timing_res.txt
rm $RES_FILE
touch $RES_FILE

Gen_tc() {
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
}


Gen_pointsto() {
    N=$N                        # number of instructions of each type
    O=`expr $N / 10`            # number of objects 
    V=`expr $N / 4`             # number of variables
    F=10                        # number of fields


    # create fact files as needed
    #             | name | |entries| |       ranges        |
    gen_fact_file   assignAlloc  $N    $V $O
    gen_fact_file   primitiveAssign  $N  $V $V
    gen_fact_file      load      $N    $V $V $F
    gen_fact_file      store     $N    $V $V $F

}

echo "=====================starting new iterations===================" >> apps/erlog/bench/results/timing_res.txt
for N in {50..400..50}
    do
        Gen_tc
        echo "\n==============new timings===================" >> ../results/timing_res.txt
        echo "graph size $N" >> ../results/timing_res.txt

        cd ../../../../
        echo "going back to `pwd`"
        rebar3 compile

        echo "now running program"
        erl -pa "_build/default/lib/erlog/ebin" -pa "_build/default/lib/erlog/bench" -noshell -noinput -eval 'timing:start(), erlang:halt()' &
        wait $!

    done

echo "=========================done===============================" >> apps/erlog/bench/results/timing_res.tx