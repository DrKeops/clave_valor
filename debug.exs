## AUTOR: Isak Edo Vivancos y Luis Fueris Martin
## NIA: 682405 - 699623
## FICHERO: debug.exs
## TIEMPO: 
## DESCRIPCION: fichero de depuración 

defmodule Debug do

    @moduledoc """
        modulo del depuracion
    """
    ##### Mensaje que envia nodo origen a nodo destino
    @spec send(pid, pid, [])    :: none 
    def send(nodo_origen, nodo_destino, mensaje) do
        :io.format("Realizando SEND desde nodo ~p con destinatario ~p 
            con mensaje ~p~n", [nodo_origen, nodo_destino, mensaje])
    end

    ##### Mensaje que recibe nodo destino desde nodo origen 
    @spec recv(pid, pid, [])    :: none
    def recv(nodo_origen, nodo_destino, mensaje) do
       :io.format("Mensaje RECV ~p recibido por ~p de nodo ~p~n", 
                        [mensaje, nodo_destino, nodo_origen]) 
    end

    ##### Mensaje especial con algun tipo de identificador
    @spec msg(String.t, [])     :: none
    def msg(id, mensaje) do
        :io.format("~p ----> ~p~n", 
                        [id, mensaje])
    end

    ##### Mensaje generico para ver si entra en una determinada condicion
    @spec msg_generico(String.t)    :: none
    def msg_generico(mensaje) do
        :io.format("Entra en ~p~n",[mensaje])
    end

end
