-module(naive).

-compile(export_all).

-include("../include/data_repr.hrl").

% reachable(X, Y) :- link(X, Y).
% reachable(X,Y) :- reachable(X, Z), link(Z, Y).
%
% EDB
% link(a, b). link(b,c). link(c,d).
%
%
%
-spec project([dl_atom()], [integer()]) -> [dl_atom()].
project(DB_instance, Cols) ->
  lists:map(fun(Atom = #dl_atom{args = Args}) ->
               Atom#dl_atom{args = utils:filteri(fun(I) -> lists:member(I, Cols) end, Args)}
            end,
            DB_instance).

-spec join(dl_knowledgebase(), string(), string(), integer(), integer(), string()) ->
            [dl_atom()].
join(Kb, Pred_sym1, Pred_sym2, C1, C2, Res) ->
  Atoms1 = lists:filter(fun(#dl_atom{pred_sym = Sym}) -> Pred_sym1 =:= Sym end, Kb),
  Atoms2 = lists:filter(fun(#dl_atom{pred_sym = Sym}) -> Pred_sym2 =:= Sym end, Kb),
  lists:flatmap(fun(Atom) -> join_one(Atoms2, C1, C2, Res, Atom) end, Atoms1).

-spec join_one([dl_atom()], integer(), integer(), string(), dl_atom()) -> [dl_atom()].
join_one(Dl_atoms, C1, C2, Res, #dl_atom{args = Args}) ->
  Nth = lists:nth(C1, Args),
  Selected_atoms =
    lists:filter(fun(#dl_atom{args = Args2}) -> lists:nth(C2, Args2) =:= Nth end, Dl_atoms),
  Selected_args =
    lists:map(fun(#dl_atom{args = Args2}) ->
                 {L, [_ | R]} = lists:split(C2 - 1, Args2),
                 L ++ R
              end,
              Selected_atoms),
  lists:map(fun(Args2) -> #dl_atom{pred_sym = Res, args = Args ++ Args2} end,
            Selected_args).

-spec start() -> dl_program().
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
  Program.
