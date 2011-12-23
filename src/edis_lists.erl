%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc New lists implementation with O(1) for lists:length/1 and some other
%%%      behaviour changes
%%% @end
%%%-------------------------------------------------------------------
-module(edis_lists).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-record(edis_list, {size  = 0   :: non_neg_integer(),
                    list  = []  :: list()}).
-opaque edis_list(T) :: #edis_list{list :: [T]}.
-export_type([edis_list/1]).

-export([length/1, from_list/1, to_list/1, nth/2, nthtail/2, reverse/1, splitwith/2, insert/4,
         append/2, push/2, pop/1, sublist/3, filter/2, remove/3, replace_head/2, split/2, empty/0]).

%% @doc returns an empty edis_list 
-spec empty() -> edis_list(_).
empty() -> #edis_list{}.

%% @doc returns the edis_list size
-spec length(edis_list(_)) -> non_neg_integer().
length(#edis_list{size = S}) -> S.

%% @doc returns an edis_list with the same elements of the list
-spec from_list([T]) -> edis_list(T).
from_list(L) -> #edis_list{size = erlang:length(L), list = L}.

%% @doc returns a list with the same elements of the edis_list
-spec to_list(edis_list(T)) -> [T].
to_list(#edis_list{list = L}) -> L.

%% @doc returns the Nth element of the list L
-spec nth(pos_integer(), edis_list(T)) -> T | undefined.
nth(N, #edis_list{size = S}) when S < N -> undefined;
nth(N, #edis_list{list = L}) -> lists:nth(N, L).

%% @doc returns the Nth tail of the list L
-spec nthtail(non_neg_integer(), edis_list(T)) -> edis_list(T).
nthtail(N, #edis_list{size = S}) when S < N -> [];
nthtail(N, #edis_list{list = L}) -> lists:nthtail(N, L).

%% @doc reverses all elements in the list L.
-spec reverse(edis_list(T)) -> edis_list(T).
reverse(EL = #edis_list{list = L}) -> EL#edis_list{list = lists:reverse(L)}.

%% @doc partition edis_list into two edis_lists according to Pred
-spec splitwith(fun((T) -> boolean()), edis_list(T)) -> {edis_list(T), edis_list(T)}.
splitwith(_Pred, #edis_list{size = 0}) -> {#edis_list{}, #edis_list{}};
splitwith(Pred, #edis_list{size = S, list = L}) ->
  {L0, L1} = lists:splitwith(Pred, L),
  EL0 = from_list(L0),
  {EL0, #edis_list{size = S - EL0#edis_list.size, list = L1}}.

%% @doc insert a value before or after the pivot
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

%% @doc Returns an edis_list of all elements Elem in the received edis_list for which Pred(Elem) returns true.
-spec filter(fun((T) -> boolean()), edis_list(T)) -> edis_list(T).
filter(_Pred, #edis_list{size = 0}) -> #edis_list{};
filter(Pred, #edis_list{list = L}) -> from_list(lists:filter(Pred, L)).

%% @doc Removes the first N elements which compares equal to the value
-spec remove(T, non_neg_integer(), edis_list(T)) -> edis_list(T).
remove(_, 0, EL) -> EL;
remove(_, _, #edis_list{size = 0}) -> empty();
remove(X, Count, EL) -> remove(X, Count, EL#edis_list.list, EL#edis_list.size, []).

remove(_X, 0, End, Size, RevStart) -> #edis_list{list = lists:reverse(RevStart) ++ End, size = Size};
remove(_X, _C, [], Size, RevStart) -> #edis_list{list = lists:reverse(RevStart), size = Size};
remove(X, C, [X|Rest], Size, RevStart) -> remove(X, C-1, Rest, Size-1, RevStart);
remove(X, C, [Y|Rest], Size, RevStart) -> remove(X, C, Rest, Size, [Y|RevStart]).

%% @doc replaces head element
-spec replace_head(T0, edis_list(T)) -> edis_list(T0|T).
replace_head(H, EL = #edis_list{list = [_|Rest]}) -> EL#edis_list{list = [H|Rest]}.

%% @doc Splits the received list into two lists. The first list contains the first N elements and the other list the rest of the elements (the Nth tail)
-spec split(non_neg_integer(), edis_list(T)) -> {edis_list(T), edis_list(T)}.
split(N, EL = #edis_list{size = S}) when N > S -> {EL, #edis_list{}};
split(N, #edis_list{size = S, list = L}) ->
  {L0, L1} = lists:split(N, L),
  {#edis_list{size = N, list = L0}, #edis_list{size = S - N, list = L1}}.
