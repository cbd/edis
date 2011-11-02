%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc New lists implementation with O(1) for lists:length/1 and some other
%%%      behaviour changes
%%% @end
%%%-------------------------------------------------------------------
-module(edis_lists).
-extends(gb_trees).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-record(edis_list, {size  = 0   :: non_neg_integer(),
                    list  = []  :: list()}).
-opaque edis_list(T) :: #edis_list{list :: [T]}.
-export_type([edis_list/1]).

-export([length/1, from_list/1, to_list/1, nth/2, nthtail/2, reverse/1, splitwith/2, insert/4,
         append/2, push/2, pop/1, sublist/3, filter/2, subtract/2, duplicate/2, replace_head/2,
         split/2, empty/0]).

-spec empty() -> edis_list(_).
empty() -> #edis_list{}.

-spec length(edis_list(_)) -> non_neg_integer().
length(#edis_list{size = S}) -> S.

-spec from_list([T]) -> edis_list(T).
from_list(L) -> #edis_list{size = erlang:length(L), list = L}.

-spec to_list(edis_list(T)) -> [T].
to_list(#edis_list{list = L}) -> L.

%% @doc returns the N`th element of the list L
-spec nth(pos_integer(), edis_list(T)) -> T | undefined.
nth(N, #edis_list{size = S}) when S < N -> undefined;
nth(N, #edis_list{list = L}) -> lists:nth(N, L).

%% @doc returns the N`th tail of the list L
-spec nthtail(non_neg_integer(), edis_list(T)) -> edis_list(T).
nthtail(N, #edis_list{size = S}) when S < N -> [];
nthtail(N, #edis_list{list = L}) -> lists:nthtail(N, L).

%% @doc reverse all elements in the list L.
-spec reverse(edis_list(T)) -> edis_list(T).
reverse(EL = #edis_list{list = L}) -> EL#edis_list{list = lists:reverse(L)}.

-spec splitwith(fun((T) -> boolean()), edis_list(T)) -> {edis_list(T), edis_list(T)}.
splitwith(_Pred, #edis_list{size = 0}) -> {#edis_list{}, #edis_list{}};
splitwith(Pred, #edis_list{size = S, list = L}) ->
  {L0, L1} = lists:splitwith(Pred, L),
  EL0 = from_list(L0),
  {EL0, #edis_list{size = S - EL0#edis_list.size, list = L1}}.

-spec insert(T, before|'after', T, edis_list(T)) -> edis_list(T).
insert(Value, Position, Pivot, EL = #edis_list{size = S, list = L}) ->
  case {lists:splitwith(fun(Val) -> Val =/= Pivot end, L), Position} of
    {{_, []}, _} -> %% Pivot was not found
      EL;
    {{Before, After}, before} ->
      #edis_list{size = S + 1, list = lists:append(Before, [Value|After])};
    {{Before, [Pivot|After]}, 'after'} ->
      #edis_list{size = S + 1, list = lists:append(Before, [Pivot, Value|After])}
  end.

%% @doc appends lists X and Y
-spec append(edis_list(T0), edis_list(T1)) -> edis_list(T0|T1).
append(#edis_list{size = S0, list = L0}, #edis_list{size = S1, list = L1}) ->
  #edis_list{size = S0+S1, list = lists:append(L0,L1)}.

%% @doc inserts X to the left of the list
-spec push(T0, edis_list(T)) -> edis_list(T|T0).
push(X, #edis_list{size = S, list = L}) -> #edis_list{size = S + 1, list = [X|L]}.

%% @doc pops the leftmost element of the list
%% @throws empty
-spec pop(edis_list(T)) -> {T, edis_list(T)}.
pop(#edis_list{size = 0}) -> throw(empty);
pop(#edis_list{size = S, list = [X|L]}) -> {X, #edis_list{size = S-1, list = L}}.

%% @doc Returns the sub-list starting at Start of length Length.
-spec sublist(edis_list(T), pos_integer(), non_neg_integer()) -> edis_list(T).
sublist(#edis_list{size = S}, Start, _Len) when Start > S -> #edis_list{};
sublist(#edis_list{size = S, list = L}, Start, Len) ->
  #edis_list{size = erlang:max(S - Start + 1, Len), list = lists:sublist(L, Start, Len)}.

-spec filter(fun((T) -> boolean()), edis_list(T)) -> edis_list(T).
filter(_Pred, #edis_list{size = 0}) -> #edis_list{};
filter(Pred, #edis_list{list = L}) -> from_list(lists:filter(Pred, L)).

-spec subtract(edis_list(T), edis_list(T)) -> edis_list(T).
subtract(#edis_list{size = 0}, _) -> #edis_list{};
subtract(EL0, #edis_list{size = 0}) -> EL0;
subtract(#edis_list{list = L0}, #edis_list{list = L1}) -> from_list(L0 -- L1).

%% @doc return N copies of X
-spec duplicate(non_neg_integer(), T) -> edis_list(T).
duplicate(N, X) -> #edis_list{size = N, list = lists:duplicate(N, X)}.

%% @doc replaces head element
-spec replace_head(T0, edis_list(T)) -> edis_list(T0|T).
replace_head(H, EL = #edis_list{list = [_|Rest]}) -> EL#edis_list{list = [H|Rest]}.

-spec split(non_neg_integer(), edis_list(T)) -> {edis_list(T), edis_list(T)}.
split(N, EL = #edis_list{size = S}) when N > S -> {EL, #edis_list{}};
split(N, #edis_list{size = S, list = L}) ->
  {L0, L1} = lists:split(N, L),
  {#edis_list{size = N, list = L0}, #edis_list{size = S - N, list = L1}}.
