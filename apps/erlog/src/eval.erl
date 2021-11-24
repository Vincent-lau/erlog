-module(eval).

-compile(export_all).

-include("../include/data_repr.hrl").

-include_lib("../include/log_utils.hrl").

-import(db_ops, [db_to_string/1]).
-import(dl_repr, [get_rule_head/1, get_atom_name/1]).

%%----------------------------------------------------------------------
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
%% Function: get_overlap_cols
%% Purpose:
%% Args: Args1 and Args2 Two lists of arguments
%% Returns: {L1,L2}, where each L1 contains the col# in L1 that overlaps with Args2
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
  Proj = db_ops:project(Atoms, Cols),
  db_ops:rename_pred(Name, Proj).

%%----------------------------------------------------------------------
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
%%----------------------------------------------------------------------
-spec eval_one_rule(dl_rule(), dl_db_instance()) -> dl_db_instance().
% PROJECTION
eval_one_rule(#dl_rule{head = Head, body = [B1 = #dl_atom{}]}, IDB) ->
  % first find all relations with the same pred as the rule in IDB
  % then need to find columns that needs to be projected
  Atoms = db_ops:get_rel_by_pred(B1#dl_atom.pred_sym, IDB),
  Cols = get_proj_cols(dl_repr:get_atom_args(Head), dl_repr:get_atom_args(B1)),
  project_onto_head(Atoms, Cols, Head#dl_atom.pred_sym);
% PRODUCT and JOIN
eval_one_rule(Rule = #dl_rule{head = Head, body = [A1 = #dl_atom{}, A2 = #dl_atom{}]},
              IDB) ->
  case get_overlap_cols(A1#dl_atom.args, A2#dl_atom.args) of
    % {[], []} ->
    % PRODUCT
    % join becomes product when there is no overlap
    % NewAtoms = db_ops:product(IDB, A1#dl_atom.pred_sym, A2#dl_atom.pred_sym),
    % Cols = get_proj_cols(dl_repr:get_atom_args(Head), A1#dl_atom.args ++ A2#dl_atom.args),
    % project_onto_head(NewAtoms, Cols, Head#dl_atom.pred_sym);
    {L1, L2} ->
      % JOIN
      % e.g. J(A, B, C, D) :- R(A, B, C), S(B, C, D).
      NewAtoms =
        db_ops:join(IDB,
                    A1#dl_atom.pred_sym,
                    A2#dl_atom.pred_sym,
                    L1,
                    L2,
                    Rule#dl_rule.head#dl_atom.pred_sym),
      ?LOG_DEBUG(#{rule => utils:to_string(Rule),
                   atoms_to_be_joined => db_ops:db_to_string(IDB),
                   c1 => L1,
                   c2 => L2}),
      ?LOG_DEBUG(#{joined_atoms => db_ops:db_to_string(NewAtoms)}),
      Cols =
        get_proj_cols(dl_repr:get_atom_args(Head),
                      preproc:combine_args([A1#dl_atom.args, A2#dl_atom.args])),
      project_onto_head(NewAtoms, Cols, Head#dl_atom.pred_sym)
  end.

%% this function applies all rules to the IDB once and return the new DB instance
-spec imm_conseq(dl_program(), dl_db_instance()) -> dl_db_instance().
imm_conseq(Program, IDB) ->
  % Instance is a list of dl_db_instance()
  Instance = lists:map(fun(Rule) -> eval_one_rule(Rule, IDB) end, Program),
  db_ops:flatten(Instance).

-spec is_fixpoint(dl_db_instance(), dl_db_instance()) -> boolean().
is_fixpoint(OldDB, NewDB) ->
  db_ops:equal(OldDB, NewDB).

is_fixpoint(NewDB) ->
  db_ops:is_empty(NewDB).

%%----------------------------------------------------------------------
%% Function: static_relation
%% Purpose: Given a rule, return whether the head of the rule is a static
%% relation. A static relation is one whose body atoms are completely in
%% the EDB and there will be no new atoms generated in later iterations.
%% E.g. reachable(X, Y) :- link(X, Y) -> true
%% Args:
%% Returns:
%%----------------------------------------------------------------------
-spec static_relation(dl_rule(), dl_program()) -> boolean().
static_relation(#dl_rule{body = Body}, Rules) ->
  HeadNames =
    sets:from_list(
      lists:map(fun(R) -> get_atom_name(get_rule_head(R)) end, Rules)),
  lists:all(fun(Atom) -> not sets:is_element(get_atom_name(Atom), HeadNames) end, Body).

%%----------------------------------------------------------------------
%% Function: find_static_db
%% Purpose: Given a rule, return whether the head of the rule is a static
%% relation by looking at its body. If ALL body atoms appear in the EDB,
%% then yes.
%% E.g. reachable(X, Y) :- link(X, Y) -> true
%% Args:
%% Returns:
%%----------------------------------------------------------------------

-spec find_static_db(dl_program(), dl_db_instance(), dl_db_instance()) ->
                      dl_db_instance().
find_static_db(Program, EDB, OneIterDB) ->
  StaticNames =
    lists:filtermap(fun(Rule = #dl_rule{head = #dl_atom{pred_sym = PS}}) ->
                       case static_relation(Rule, Program) of
                         false -> false;
                         true -> {true, PS}
                       end
                    end,
                    Program),
  StaticDBL =
    lists:map(fun(Name) -> db_ops:get_rel_by_pred(Name, OneIterDB) end, StaticNames),
  db_ops:add_db_unique(
    db_ops:flatten(StaticDBL), EDB).

%% calls eval one until a fixpoint is reached
%% returns the final db instance
-spec eval_all(dl_program(), dl_db_instance()) -> dl_db_instance().
eval_all(Program, EDB) ->
  ?LOG_DEBUG(#{initial_db => db_to_string(EDB)}),
  NewDB = imm_conseq(Program, EDB),
  FullDB = db_ops:add_db_unique(NewDB, EDB),
  % static db should be complete after one iteration
  StaticDB = FullDB,
  ?LOG_DEBUG(#{static_db => db_to_string(StaticDB)}),

  NonStaticProg = lists:filter(fun(R) -> not static_relation(R, Program) end, Program),

  ?LOG_DEBUG(#{non_static_rules => utils:to_string(NonStaticProg)}),
  eval_seminaive(NonStaticProg, FullDB, StaticDB, NewDB).

eval_naive(Program, EDB) ->
  NewDB = imm_conseq(Program, EDB),
  ?LOG_DEBUG(#{after_imm_cq_db => db_ops:db_to_string(NewDB)}),
  FullDB = db_ops:add_db_unique(EDB, NewDB),
  ?LOG_DEBUG(#{added_new_tuples_to_db => db_ops:db_to_string(FullDB)}),

  case is_fixpoint(FullDB, EDB) of
    true ->
      FullDB;
    false ->
      eval_naive(Program, FullDB)
  end.

-spec eval_seminaive(dl_program(),
                     dl_db_instance(),
                     dl_db_instance(),
                     dl_db_instance()) ->
                      dl_db_instance().
eval_seminaive(Program, FullDB, StaticDB, DeltaDB) ->
  ?LOG_DEBUG(#{current_full_db => db_ops:db_to_string(FullDB)}),
  ?LOG_DEBUG(#{current_delta => db_ops:db_to_string(DeltaDB)}),
  GeneratedDB = imm_conseq(Program, db_ops:add_db_unique(StaticDB, DeltaDB)),
  NewFullDB = db_ops:add_db_unique(FullDB, DeltaDB),
  NewDB = db_ops:diff(GeneratedDB, NewFullDB),
  ?LOG_DEBUG(#{new_facts_learned => db_ops:db_to_string(NewDB)}),
  ?LOG_DEBUG(#{added_delta_tuples_to_db => db_ops:db_to_string(FullDB)}),

  case is_fixpoint(NewDB) of
    true ->
      NewFullDB;
    false ->
      eval_seminaive(Program, NewFullDB, StaticDB, NewDB)
  end.

% reachable(X, Y) :- link(X, Y).
% reachable(X,Y) :- reachable(X, Z), link(Z, Y).
%
% EDB
% link(a, b). link(b,c). link(c,d).
%
-spec start() -> dl_db_instance().
start() ->
  R1 =
    #dl_rule{head = #dl_atom{pred_sym = reachable, args = ["X", "Y"]},
             body = [#dl_atom{pred_sym = link, args = ["X", "Y"]}]},
  R2 =
    #dl_rule{head = #dl_atom{pred_sym = reachable, args = ["X", "Y"]},
             body =
               [#dl_atom{pred_sym = reachable, args = ["X", "Z"]},
                #dl_atom{pred_sym = link, args = ["Z", "Y"]}]},
  Program = [R1, R2],
  EDB = [],
  eval_all(Program, EDB).
