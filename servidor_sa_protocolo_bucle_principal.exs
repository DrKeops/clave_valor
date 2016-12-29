
defmodule ServidorSA do
    
            
    defstruct  primario: :undefined, copia: :undefined, servidor_gv: :undefined, 
            nv_conocido: 0, bbdd: Map.new()


    @intervalo_latido 50

    @doc """
        Poner en marcha un servidor de almacenamiento
    """
    @spec start(String.t, String.t, node) :: node
    def start(host, nombre_nodo, nodo_servidor_gv) do
        nodo = NodoRemoto.start(host, nombre_nodo, __ENV__.file, __MODULE__)
        
        Node.spawn(nodo, __MODULE__, :init_sa, [nodo_servidor_gv])

        nodo
    end


    #------------------- Funciones privadas -----------------------------

    defp struct_inicial() do
        %ServidorSA{}
    end

    def init_sa(nodo_servidor_gv) do
        Process.register(self(), :servidor_sa)
        # Process.register(self(), :cliente_gv)
        spawn(__MODULE__, :init_monitor, [self()])

        # Poner estado inicial
        estado = struct_inicial()
        bucle_recepcion_principal(%{ estado | servidor_gv: nodo_servidor_gv}) 
    end


    ### Función para inicializar el proceso que mantiene la situación de los 
    #   servidores en [procesa_situacion_servidores]
    def init_monitor(pid_principal) do
        send(pid_principal, :envia_latido)
        Process.sleep(@intervalo_latido)
        init_monitor(pid_principal)
    end
 

    defp actualizar_estado_vista_tentativa(estado, vista, valida) do
        # Si es válida actualizamos nuestro estado con el primario, copia
        # y num vista tentativa
        if valida == true do
            %{estado | primario: vista.primario, copia: vista.copia, 
                        nv_conocido: vista.num_vista}
            
        else
            %{estado | primario: :undefined, copia: :undefined, 
                        nv_conocido: 0}

        end
        
    end


    defp procesar_lectura(clave, nodo_origen, estado) do
        #### FALTA NO SOY PRIMARIO VALIDO EN EL SEND SI EL NUMERO DE VISTA
        #### NO ES IGUAL QUE EL TIENE 
        result = Map.get(estado.bbdd, clave) 
        result = if result == nil do "" else result end
        #:io.format("El nodo ~p ha leido ~p~n", [nodo_origen, result])

        send({:cliente_sa, nodo_origen}, {:resultado, result})
 
    end

    defp estado_escribir(param, estado, cliente) do

        estado = receive do

            {:ok_escritura, _nodo_origen}    ->
                # Cuando recibimos el ok, procesamos la escritura
                #:io.format("Recibimos OK copia ~p~n", [nodo_origen])
                procesar_escritura_primario(param, cliente, estado)

            :no_soy_copia_valido    ->
                estado

            {:vista_tentativa, _vista, _valida}   ->
                estado_escribir(param, estado, cliente)                

            # Se sigue enviando el látido
            :envia_latido             ->
                #:io.format("Entra en envia latido")
                send({:servidor_gv, estado.servidor_gv}, 
                            {:latido, Node.self(), estado.nv_conocido })

                estado_escribir(param, estado, cliente)

        after @intervalo_latido ->
            estado

        end

        estado

    end

    defp procesar_escritura_primario(param, nodo_origen, estado) do
        # Esta es la tupla que nos envían {clave, nuevo_valor, con_hash}
        # Miramos si la clave enviada tiene valor asignado
        result = Map.get(estado.bbdd, elem(param, 0))
        result = if result == nil do "" else result end

        # Se actualiza la base de datos
        # En el update, el tercer parámetro, si no encuentra la clave pone
        # ese valor por defecto.
        estado = %{estado | bbdd: Map.update(estado.bbdd, elem(param,0), 
                elem(param,1), fn(_) -> elem(param, 1) end)} 

        #:io.format("Estado actual primario ~p~n", [estado])
        # Se contesta con el resultado que había en el estado
        send({:cliente_sa, nodo_origen}, {:resultado, result})
        #:io.format("Resultado ~p enviado a ~p~n", [result, nodo_origen])

        estado        
    end


    defp procesar_escritura_copia(param, estado) do
        # Unicamente hacemos el update de la base de datos
        %{estado | bbdd: Map.update(estado.bbdd, elem(param,0), 
                elem(param, 1), fn(_) -> elem(param, 1) end)} 
        
    end

    defp bucle_recepcion_principal(estado) do

        estado = receive do

            {:vista_tentativa, vista, valida}   ->
                actualizar_estado_vista_tentativa(estado, vista, valida)
            
            # Primario realiza un backup en copia, lo recibe el copia 
            {:copia_backup, bbdd, nodo_origen}  ->
                estado = if Node.self() != estado.copia do
                    # No soy copia válida
                    send({:servidor_sa, nodo_origen}, {:no_soy_copia_valida})

                    estado
                else
                    # Realizamos backup del copia
                    %{estado | bbdd: Map.new(bbdd)}

                end 

                estado

            # Solicitud de primario a copia para que copie param
            {:copia_escribe, param, nodo_origen}    ->
                estado = if Node.self() == estado.copia do
                    estado = procesar_escritura_copia(param, estado)
                    send({:servidor_sa, nodo_origen}, {:ok_escritura, Node.self()})

                    estado
                else
                    send({:servidor_sa, nodo_origen}, {:no_soy_copia_copia})

                    estado
                end

                estado

            # Solicitudes de lectura y escritura de clientes del servicio almace.    
            {op, param, nodo_origen}    ->
                if Node.self() == estado.primario do
                    if op == :lee do
                        procesar_lectura(param, nodo_origen, estado)
                    end
                    
                    if op == :escribe_generico do
                        #:io.format("Entra en escribr generico para copia~n")
                        send({:servidor_sa, estado.copia}, 
                            {:copia_escribe, param, Node.self()})
                        # Envíamos solicitud de escritura al nodo copia
                        estado_escribir(param, estado, nodo_origen)
                    else
                        estado    
                    end
                    
                else
                    # No soy primario válido pero me llega una operación
                    send({:cliente_sa, nodo_origen}, 
                        {:resultado, :no_soy_primario_valido})

                    estado
                end

            :envia_latido             ->
                send({:servidor_gv, estado.servidor_gv}, 
                            {:latido, Node.self(), estado.nv_conocido })
                estado

        end

        bucle_recepcion_principal(estado)
    end
end
