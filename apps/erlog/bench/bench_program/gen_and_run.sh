#!/usr/bin/env zsh

# create a fresh result file
RES_FILE=apps/erlog/bench/results/timing_res.txt
# rm $RES_FILE
# touch $RES_FILE

Gen_tc() {
    echo "generating tc program"
    echo "current size of graph, i.e. value of N is $N"

    C=$N
    SIZE=$N
    E=`expr $C \* 10`    # each node has on average 10 neighbors

    # create fact files as needed
    #             | name | |entries| |       ranges        |
    gen_fact_file   base      $E    $C $C

    cp ./tc_program.dl ./tc_bench.dl
    ./to_atoms.py tc >> ./tc_bench.dl
    echo "graph generated into tc_bench.dl"
}

Gen_scc() {
    echo "generating scc program"
    echo "current size of graph, i.e. value of N is $N"

    C=$N
    SIZE=$N
    E=`expr $C \* 10`    # each node has on average 10 neighbors

    # create fact files as needed
    #             | name | |entries| |       ranges        |
    gen_fact_file   base      $E    $C $C

    cp ./scc_program.dl ./scc_bench.dl
    ./to_atoms.py tc >> ./scc_bench.dl
    echo "graph generated into scc_bench.dl"
}


Gen_pointsto() {
    echo "generating pointsto program"
    echo "current size is $N"

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

    cp ./pointsto_program.dl ./pointsto_bench.dl
    ./to_atoms.py pointsto >> pointsto_bench.dl 
    echo "generated pointsto_bench.dl"

}

Gen_rsg() {
    echo "generating rsg program"
    echo "current size is $N"

    N=$N
    F=`expr $N \* 3`
    U=`expr $N \* 3`
    D=`expr $N \* 3`

    gen_fact_file flat $F $N $N
    gen_fact_file up $U $N $N
    gen_fact_file down $D $N $N

    cp ./rsg_program.dl ./rsg_bench.dl
    ./to_atoms.py rsg >> rsg_bench.dl 
    echo "generated rsg_bench.dl"

}


Gen_cite() {
    echo "generating data from citation network real data program"
    echo "current size of graph, i.e. value of N is $N"

    # subset the citation network data
    C=`expr $N \* 10`
    python3 subset.py $C

    cp ./tc_program.dl ./tc_bench.dl
    cat facts/citations.facts >> ./tc_bench.dl
    echo "citation graph generated into tc_bench.dl"

}

echo "=====================starting new iterations===================" >> $RES_FILE
for N in {50..50..50}
    do
        cd apps/erlog/bench/bench_program
        . ./utils.sh

        Gen_cite
        echo "\n==============new timings===================" >> ../results/timing_res.txt
        echo "graph size $N" >> ../results/timing_res.txt

        cd ../../../../
        echo "going back to `pwd`"
        rebar3 compile

        # echo "now running program"
        # erl -pa "_build/default/lib/erlog/ebin" \
        #     -pa "_build/default/lib/erlog/bench" \
        #     -config "config/sys.config" \
        #     -noshell -noinput \
        #     -eval 'application:start(erlog), timing:start(), application:stop(erlog), erlang:halt()' \
        #     &
        # wait $!

    done

# echo "=========================done===============================" >> $RES_FILE
