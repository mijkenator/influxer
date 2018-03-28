-module(batcher_sup).

-export([
         start_link/0,
         start_child/2
        ]).

-behaviour(supervisor).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, {}).

start_child(Name, Args) when is_atom(Name) ->
    ChildSpec = {batcher_worker, {batcher_worker, start_link, [[Name|Args]]}, temporary, 2000, worker, [batcher_worker]},
    supervisor:start_child(?MODULE, ChildSpec).

init({}) ->
	RestartStrategy = {one_for_one, 5, 10},
    {ok, {RestartStrategy, []}}.
