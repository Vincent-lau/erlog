-module(naive).

-compile(export_all).

-include("data_repr.hrl").

%% we consider the simple case first
%% two relations, assuming that the column to be joined is the last of the first
%% and the first of the last
%%
%% but need to consider partition relations later when there are more than 1
%% and also find the column to be join, or rearranging the column so that the
%% pre-condition holds
-spec eval_one_rule(dl_rule(), dl_db_instance()) -> dl_db_instance().
eval_one_rule(Rule = #dl_rule{body = [R1 = #dl_atom{}, R2 = #dl_atom{}]}, IDB) ->
  Delta =
    db_ops:join(IDB,
                R1#dl_atom.pred_sym,
                R2#dl_atom.pred_sym,
                length(R1#dl_atom.args),
                1,
                Rule#dl_rule.head#dl_atom.pred_sym),
  db_ops:project(Delta, [1, 3]);
eval_one_rule(Rule = #dl_rule{body = [R1 = #dl_atom{}]}, IDB) ->
  % first find all relations with the same pred as the rule in IDB
  % then need to find columns that needs to be projected
  [].

%% this function applies rules to the IDB once and return the new DB instance
-spec eval_one_iter(dl_program(), dl_db_instance()) -> dl_db_instance().
eval_one_iter(Program, IDB) ->
  lists:flatmap(fun(Rule) -> eval_one_rule(Rule, IDB) end, Program).

% TODO list ordering matter, in general list is not a good choice, consider using set
-spec is_fixpoint(dl_db_instance(), dl_db_instance()) -> atom().
is_fixpoint(OldDB, NewDB) ->
  OldDB =:= NewDB.

%% calls eval one until a fixpoint is reached
%% returns the final db instance
-spec eval_all(dl_program(), dl_db_instance()) -> dl_db_instance().
eval_all(Program, IDB) ->
  NewDB = eval_one_iter(Program, IDB),
  FullDB = IDB ++ NewDB,
  case is_fixpoint(NewDB, IDB) of
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
