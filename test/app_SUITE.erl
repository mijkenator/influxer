-module(app_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
            all_exists/1,
            apps/1,
            supervisor_tree/1,
            start_worker/1
        ]).

init_per_suite(_Config) ->
    {ok, _} = application:ensure_all_started(influxer),
    [].

end_per_suite(_Config) ->
    ok = application:stop(influxer).

all() -> [
        all_exists, apps, supervisor_tree, start_worker
    ].

all_exists(_Config) ->
    Modules = [
                batcher_sup, 
                batcher_worker,
                influxer_app,
                influxer_sup,
                influxer_utils
              ],
    check_modules(Modules).

check_modules([])    -> true;
check_modules([H|T]) -> 
    case code:load_file(H) of
        {module, H} -> check_modules(T)
        ;Error ->
            io:format("Module ~p load failed (~p)", [H, Error]),
            false
    end.

apps(_Config) ->
    AL = [An || {An,_,_} <- application:which_applications()],
    Fun = fun(A) -> true = lists:member(A, AL) end,
    lists:foreach(Fun, [buoy,lager,influxer]).

supervisor_tree(_Config) ->
    CL = [Cn || {Cn,_,_,_} <- supervisor:which_children(influxer_sup)],
    Fun = fun(C) -> true = lists:member(C, CL) end,
    lists:foreach(Fun, [batcher_sup]).

start_worker(_Config) ->
    [] = supervisor:which_children(batcher_sup),
    batcher_sup:start_child(b1, [<<"http://10.0.0.85:8086/write?db=mkh_data">>, <<"root">>,<<"Rbccm2018">>,1,2000,1000]),
    [{batcher_worker,_,worker,[batcher_worker]}] = supervisor:which_children(batcher_sup).


