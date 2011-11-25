%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc DB backend behaviour for edis.
%%%<ul>
%%%   <li>
%%%   <pre>init(string(), non_neg_integer(), eleveldb:open_options()) -> {ok, ref()} | {error, term()}</pre>
%%%     Opens and/or initializes the backend<br/>
%%%   </li><li>
%%%   <pre>write(ref(), edis_backend:write_actions()) -> ok | {error, term()}</pre>  
%%%      Atomically apply a set of actions<br/>
%%%   </li><li>
%%%   <pre>put(ref(), binary(), #edis_item{}) -> ok | {error, term()}</pre>  
%%%     Puts an item in the database<br/>
%%%   </li><li>
%%%   <pre>delete(ref(), binary()) -> ok | {error, term()}</pre>
%%%     Deletes an item<br/>
%%%   </li><li>
%%%   <pre>is_empty(ref()) -> boolean()</pre>  
%%%     Determines if the database is empty<br/>
%%%   </li><li>
%%%   <pre>destroy(ref()) -> ok | {error, term()}</pre>  
%%%     Deletes database<br/>
%%%   </li><li>
%%%   <pre>status(ref()) -> {ok, binary()} | error</pre>  
%%%     Determines the databse status<br/>
%%%   </li><li>
%%%   <pre>get(ref(), binary()) -> #edis_item{} | not_found | {error, term()}</pre>  
%%%     Gets an item from the database<br/>
%%%   </li>
%%%</ul>
%%% @end
%%%-------------------------------------------------------------------
-module(edis_backend).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-include("edis.hrl").

-type write_actions() :: [{put, Key::binary(), Value::#edis_item{}} |
                          {delete, Key::binary()} |
                          clear].
-type fold_fun() :: fun((Item::#edis_item{}, any()) -> any()).
-export_type([write_actions/0, fold_fun/0]).

-export([behaviour_info/1]).

%% @hidden
-spec behaviour_info(callbacks|term()) -> undefined | [{atom(), non_neg_integer()}].
behaviour_info(callbacks) -> [{init, 3}, {write, 2}, {put, 3}, {delete, 2}, {is_empty, 1},
                              {destroy, 1}, {status, 1}, {get, 2}];
behaviour_info(_) -> undefined.