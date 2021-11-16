-module(eval).

-compile(export_all).

-include("../include/data_repr.hrl").

-include_lib("../include/utils.hrl").

%%----------------------------------------------------------------------
%% Function: get_proj_cols
%% Purpose: Find columns of Args2 that is in Args1, 1-based index
%% Args:
%% Returns:
%% e.g. (T, Y, L) (G, T, Y, S, L, P) -> (2, 3, 5)
%%----------------------------------------------------------------------
-spec get_proj_cols([string()], [string()]) -> [integer()].
get_proj_cols(Args1, Args2) ->
  listsi:filtermapi(fun(E, Idx) ->
                       case lists:member(E, Args1) of
                         false -> false;
                         true -> {true, Idx}
                       end
                    end,
                    Args2).

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
      ?LOG_TOPIC_DEBUG(eval_one_rule,
                       #{rule => utils:to_string(Rule),
                         atoms_to_be_joined => db_ops:db_to_string(IDB),
                         c1 => L1,
                         c2 => L2}),
      ?LOG_TOPIC_DEBUG(eval_one_rule, #{joined_atoms => db_ops:db_to_string(NewAtoms)}),
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

%% calls eval one until a fixpoint is reached
%% returns the final db instance
-spec eval_all(dl_program(), dl_db_instance()) -> dl_db_instance().
eval_all(Program, EDB) ->
  ?LOG_TOPIC_DEBUG(eval_all, #{initial_db => db_ops:db_to_string(EDB)}),
  NewDB = imm_conseq(Program, EDB),
  ?LOG_TOPIC_DEBUG(eval_all, #{after_imm_cq_db => db_ops:db_to_string(NewDB)}),
  FullDB = db_ops:add_db_unique(EDB, NewDB),
  ?LOG_TOPIC_DEBUG(eval_all, #{added_new_tuples_to_db => db_ops:db_to_string(FullDB)}),

  case is_fixpoint(FullDB, EDB) of
    true ->
      FullDB;
    false ->
      eval_all(Program, FullDB)
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
