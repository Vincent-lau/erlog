-module(eval).

-export([eval_seminaive/2, eval_all/2, get_overlap_cols/2, eval_one_rule/3,
         get_proj_cols/2]).

-include("../include/data_repr.hrl").
-include("../include/log_utils.hrl").

-ifdef(TEST).

-compile(export_all).

-endif.

-import(dl_repr,
        [get_rule_head/1, get_rule_body/1, get_atom_name/1, get_rule_headname/1]).

%%----------------------------------------------------------------------
%% @doc
%% Function: get_proj_cols
%% Purpose: Find columns of Args2 that is in Args1, 1-based index
%% Args:
%% Returns:
%% e.g. (T, Y, L) (G, T, Y, S, L, P) -> (2, 3, 5)
%% this needs to be order preserving as well, i.e. matching the head of
%% the rule
%% e.g.
%% (X, Y) :- (Y, X) -> then we should project column 2 first and then
%% column 1, and then the return value of this function should be [2, 1]
%%
%% so the strategy is to iterate over Args1 and see whether each element
%% is in Args2, if so, what is the index of that element in *Args1*
%% @end
%%----------------------------------------------------------------------
-spec get_proj_cols([string()], [string()]) -> [integer()].
get_proj_cols(Args1, Args2) ->
  lists:filtermap(fun(E) ->
                     case listsi:index_of(E, Args2) of
                       not_found -> false;
                       I -> {true, I}
                     end
                  end,
                  Args1).

%%----------------------------------------------------------------------
%% @doc
%% Function: get_overlap_cols
%% Purpose:
%% Args: Args1 and Args2 Two lists of arguments
%% @returns: {L1, L2}, where each L1 contains the col# in L1 that overlaps with Args2
%% @end
%%----------------------------------------------------------------------
get_overlap_cols(Args1, Args2) ->
  {get_proj_cols(Args2, Args1), get_proj_cols(Args1, Args2)}.

%%----------------------------------------------------------------------
%% Function: project_onto_head
%% Purpose: given a database instance that contains relations with the
%% same name, project all of them according to the cols
%% Args: Atoms as the db instance, Cols as the columns that we need to project
%% and Name as the name of the predicate onto which we project
%% Returns: a new db instance that we have the new projected db
%%
%% Example: P(A, B) :- R(A, B, C, D) so project cols are [1,2] and we
%% project everything in Atoms (which are supposed to be Rs) and rename
%% them to P
%%----------------------------------------------------------------------
-spec project_onto_head(dl_db_instance(), [integer()], dl_const()) -> dl_db_instance().
project_onto_head(Atoms, Cols, Name) ->
  Proj = dbs:project(Atoms, Cols),
  dbs:rename_pred(Name, Proj).

%%----------------------------------------------------------------------
%% @doc
%% For a given rule, this function does the join with the delta.
%% Consider the program
%%
%% ```
%% reachable(X, Y) :- link(X, Y).
%% reachable(X, Y) :- reachable(X, Z), reachable(Z, Y).
%% '''
%%
%% In this case when we evaluate the second rule, we need to actually consider
%% two rules:
%%
%% ```
%%  reachable(X, Y) :- _delta_reachable(X, Z), reachable(Z, Y).
%%  reachable(X, Y) :- reachable(X, Z), _delta_reachable(Z, Y).
%% '''
%% And so we do this one at a time, by looking at each body atom in term
%% and check whether it is a idb predicate, if so, we create a rule with
%% the "delta" form as above.
%% @end
%%----------------------------------------------------------------------

-spec join_with_delta(dl_atom(),
                      dl_atom(),
                      [integer()],
                      [integer()],
                      dl_rule(),
                      dl_program(),
                      dl_db_instance(),
                      dl_db_instance()) ->
                       dl_db_instance().
join_with_delta(Atom1, Atom2, L1, L2, Rule, Rules, FullDB, DeltaDB) ->
  PredSym1 = get_atom_name(Atom1),
  PredSym2 = get_atom_name(Atom2),
  case {is_idb_pred(Atom1, Rules), is_idb_pred(Atom2, Rules)} of
    {true, true} ->
      DB1 = dbs:get_rel_by_pred(PredSym1, DeltaDB),
      DB2 = dbs:get_rel_by_pred(PredSym2, FullDB),
      R1 = dbs:join(DB1, DB2, L1, L2, get_rule_headname(Rule)),
      DB3 = dbs:get_rel_by_pred(PredSym1, FullDB),
      DB4 = dbs:get_rel_by_pred(PredSym2, DeltaDB),
      R2 = dbs:join(DB3, DB4, L1, L2, get_rule_headname(Rule)),
      dbs:union(R1, R2);
    {true, false} ->
      DB1 = dbs:get_rel_by_pred(PredSym1, DeltaDB),
      DB2 = dbs:get_rel_by_pred(PredSym2, FullDB),
      dbs:join(DB1, DB2, L1, L2, get_rule_headname(Rule));
    {false, true} ->
      DB1 = dbs:get_rel_by_pred(PredSym1, FullDB),
      DB2 = dbs:get_rel_by_pred(PredSym2, DeltaDB),
      dbs:join(DB1, DB2, L1, L2, get_rule_headname(Rule));
    {false, false} ->
      DB1 = dbs:get_rel_by_pred(PredSym1, FullDB),
      DB2 = dbs:get_rel_by_pred(PredSym2, FullDB),
      dbs:join(DB1, DB2, L1, L2, get_rule_headname(Rule))
  end.

%%----------------------------------------------------------------------
%% @doc
%% Function: eval_one_rule
%% Purpose: evaluate a single rule
%% Args: a rule and the current IDB
%% Returns: the new IDB
%%
%% We assume that rules have already been partitioned, i.e., at most two
%% body atoms
%%
%% Need to consider PROJECTION
%% P(T, Y, L) :- Movies(T, Y, L, G, S, P).
%%
%% PRODUCT
%% P(A, B, C, X, Y, Z) :- R(A, B, C), S(X, Y, Z).
%%
%% JOIN (or more general ones)
%% join of two predicates that does not overlap is a product operation
%% and join of two predicates that completely overlaps is an intersection
%% J(A, B, C, D) :- R(A, B), S(B, C, D).
%% @end
%%----------------------------------------------------------------------
-spec eval_one_rule(dl_rule(), dl_program(), dl_db_instance(), dl_db_instance()) ->
                     dl_db_instance().
eval_one_rule(#dl_rule{head = Head, body = [B1 = #dl_atom{}]}, Rules, FullDB, DeltaDB) ->
  % PROJECTION
  % first find all relations with the same pred as the rule in IDB
  % then need to find columns that needs to be projected
  case is_idb_pred(B1, Rules) of
    true ->
      Atoms = dbs:get_rel_by_pred(get_atom_name(B1), DeltaDB);
    false ->
      Atoms = dbs:get_rel_by_pred(get_atom_name(B1), FullDB)
  end,
  Cols = get_proj_cols(dl_repr:get_atom_args(Head), dl_repr:get_atom_args(B1)),
  project_onto_head(Atoms, Cols, Head#dl_atom.pred_sym);
eval_one_rule(Rule = #dl_rule{head = Head, body = [A1 = #dl_atom{}, A2 = #dl_atom{}]},
              Rules,
              FullDB,
              DeltaDB) ->
  % PRODUCT and JOIN
  {L1, L2} = get_overlap_cols(A1#dl_atom.args, A2#dl_atom.args),
  % JOIN
  % e.g. J(A, B, C, D) :- R(A, B, C), S(B, C, D).
  NewAtoms = join_with_delta(A1, A2, L1, L2, Rule, Rules, FullDB, DeltaDB),

  ?LOG_DEBUG(#{rule => utils:to_string(Rule),
               full_db => dbs:to_string(FullDB),
               delta => dbs:to_string(DeltaDB),
               c1 => L1,
               c2 => L2}),
  ?LOG_DEBUG(#{joined_atoms => dbs:to_string(NewAtoms)}),
  Cols =
    get_proj_cols(dl_repr:get_atom_args(Head),
                  preproc:combine_args([A1#dl_atom.args, A2#dl_atom.args])),
  project_onto_head(NewAtoms, Cols, Head#dl_atom.pred_sym).

%% @doc
%% @equiv eval_one_rule(Rule, Program, DB, DB)
%% @end
eval_one_rule(Rule, Program, DB) ->
  eval_one_rule(Rule, Program, DB, DB).

%% this function applies all rules to the IDB once and return the new DB instance
-spec imm_conseq(dl_program(), dl_db_instance(), dl_db_instance()) -> dl_db_instance().
imm_conseq(Program, FullDB, DeltaDB) ->
  % Instance is a list of dl_db_instance()
  Instance =
    lists:map(fun(Rule) -> eval_one_rule(Rule, Program, FullDB, DeltaDB) end, Program),
  dbs:flatten(Instance).

%% @doc
%% @equiv imm_conseq(Program, DB, DB)
%% @end
-spec imm_conseq(dl_program(), dl_db_instance()) -> dl_db_instance().
imm_conseq(Program, DB) ->
  imm_conseq(Program, DB, DB).

-spec is_fixpoint(dl_db_instance(), dl_db_instance()) -> boolean().
is_fixpoint(OldDB, NewDB) ->
  dbs:equal(OldDB, NewDB).

is_fixpoint(NewDB) ->
  dbs:is_empty(NewDB).

%%----------------------------------------------------------------------
%% @doc
%% Given an atom/relation, return whether it is an idb predicate
%%
%% <p>
%% An edb relation is one that <em>only</em> appears in the body of
%% the rules. An intensional relation is a relation occurring in the
%% head of some rule of P.
%% </p>
%%
%% @end
%%----------------------------------------------------------------------
-spec is_idb_pred(dl_atom(), dl_program()) -> boolean().
is_idb_pred(Atom, Rules) ->
  Name = get_atom_name(Atom),
  lists:any(fun(R) -> get_rule_headname(R) =:= Name end, Rules).

-spec is_edb_pred(dl_atom(), dl_program()) -> boolean().
is_edb_pred(Atom, Rules) ->
  Name = get_atom_name(Atom),
  lists:all(fun(R) -> get_rule_headname(R) =/= Name end, Rules).


%% calls eval one until a fixpoint is reached
%% returns the final db instance
-spec eval_all(dl_program(), dl_db_instance()) -> dl_db_instance().
eval_all(Program, EDB) ->
  ?LOG_DEBUG(#{initial_db => dbs:to_string(EDB), program => Program}),
  eval_seminaive(Program, EDB).

eval_naive(Program, EDB) ->
  NewDB = imm_conseq(Program, EDB),
  ?LOG_DEBUG(#{after_imm_cq_db => dbs:to_string(NewDB)}),
  FullDB = dbs:union(EDB, NewDB),
  ?LOG_DEBUG(#{added_new_tuples_to_db => dbs:to_string(FullDB)}),

  case is_fixpoint(FullDB, EDB) of
    true ->
      FullDB;
    false ->
      eval_naive(Program, FullDB)
  end.

%%----------------------------------------------------------------------
%% @doc
%% Given a datalog program, find all rules that does not have a idb predicate
%% as its body, quoting the textbook
%% <blockquote>
%% Set P' to be the rules in P with no idb predicate in the body
%% </blockquote>
%% @end
%%----------------------------------------------------------------------
-spec get_edb_program(dl_program()) -> dl_program().
get_edb_program(Program) ->
  lists:filter(fun(Rule) ->
                  lists:all(fun(A) -> not is_idb_pred(A, Program) end, get_rule_body(Rule))
               end,
               Program).

-spec eval_seminaive(dl_program(), dl_db_instance(), dl_db_instance()) ->
                      dl_db_instance().
eval_seminaive(Program, FullDB, DeltaDB) ->
  ?LOG_DEBUG(#{current_full_db => dbs:to_string(FullDB)}),
  ?LOG_DEBUG(#{current_delta => dbs:to_string(DeltaDB)}),

  NewFullDB = dbs:union(FullDB, DeltaDB),

  GeneratedDB = imm_conseq(Program, NewFullDB, DeltaDB),
  NewDB = dbs:diff(GeneratedDB, NewFullDB),
  ?LOG_DEBUG(#{new_facts_learned => dbs:to_string(NewDB)}),
  ?LOG_DEBUG(#{added_delta_tuples_to_db => dbs:to_string(NewFullDB)}),

  case is_fixpoint(NewDB) of
    true ->
      NewFullDB;
    false ->
      eval_seminaive(Program, NewFullDB, NewDB)
  end.

eval_seminaive(Program, EDB) ->
  EDBProg = get_edb_program(Program),
  DeltaDB = imm_conseq(EDBProg, EDB, dbs:new()),
  IDBPred = dbs:filter(fun(Atom) -> is_idb_pred(Atom, Program) end, EDB),
  eval_seminaive(Program, EDB, dbs:union(DeltaDB, IDBPred)).

