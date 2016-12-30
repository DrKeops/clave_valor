Code.require_file("#{__DIR__}/debug.exs")

defmodule ServidorSA do
                
    defstruct primario: :undefined, copia: :undefined, servidor_gv: :undefined, 
    			nv_conocido: 0, bbdd: Map.new()


    @intervalo_latido 50

    defp struct_inicial() do
        %ServidorSA{}
    end

    @doc """
        Obtener el hash de un string Elixir
            - Necesario pasar, previamente,  a formato string Erlang
         - Devuelve entero
    """
    def hash(string_concatenado) do
        String.to_charlist(string_concatenado) |> :erlang.phash2
    end

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

    def init_sa(nodo_servidor_gv) do
        Process.register(self(), :servidor_sa)
        # Process.register(self(), :cliente_gv)
 
        spawn(__MODULE__, :init_monitor, [self()])

        estado = struct_inicial()

        bucle_recepcion_principal(%{estado | servidor_gv: nodo_servidor_gv}) 
    end

    def init_monitor(pid_principal) do
        send(pid_principal, :envia_latido)
        Process.sleep(@intervalo_latido)
        init_monitor(pid_principal)
    end

    # BUCLE PRINCIPAL, NODO EN ESPERA O REARRANCADO

    defp bucle_recepcion_principal(estado) do
        estado = receive do

        	:envia_latido ->
            	send({:servidor_gv, estado.servidor_gv}, 
            		{:latido, Node.self(), estado.nv_conocido})
            	estado

            {:vista_tentativa, vista, valida} ->
            	estado = actualizar_estado_vt(estado,vista,valida)
            	if(Node.self() == estado.primario) do
            		bucle_recepcion_primario(estado)
            	end
            	if(Node.self() == estado.copia) do
            		bucle_recepcion_copia(estado)
            	end
            	estado

            # Solicitudes de lectura y escritura de clientes del servicio almace
            {_op, _param, _nodo_origen}  ->
            	send({:cliente_sa, estado.servidor_gv}, {:resultado, 
            		:no_soy_primario_valido})
            	estado

	        _otro ->
	        	estado
        end

        bucle_recepcion_principal(estado)
    end

    #ESTADO PRIMARIO

    defp bucle_recepcion_primario(estado) do
        estado = receive do

        	:envia_latido ->
            	send({:servidor_gv, estado.servidor_gv}, 
            		{:latido, Node.self(), estado.nv_conocido})
            	estado

            {:vista_tentativa, vista, valida} ->
            	copia_ant = estado.copia
            	estado = actualizar_estado_vt(estado,vista,valida)
            	if(estado.copia != copia_ant && estado.copia != :undefined) do
            		send({:servidor_sa, estado.copia},{:copia_backup, 
            			estado.bbdd, Node.self()})
	            	estado_copiar(estado)
	            end
	            estado

            # Solicitudes de lectura y escritura de clientes del servicio almace
            {op, param, nodo_origen}  ->
            	if(op == :lee) do
            		procesar_lectura(estado, param, nodo_origen)
            	end
            	if(op == :escribe_generico) do
            		send({:servidor_sa, estado.copia},{:copia_escribe, 
            			param, Node.self()})
	            	estado_escribir(estado, param, nodo_origen)
            	else estado	
            	end

	        _otro ->
	        	estado
        end

        bucle_recepcion_primario(estado)
    end

    #ESTADO COPIA

    defp bucle_recepcion_copia(estado) do
        estado = receive do

        	:envia_latido ->
            	send({:servidor_gv, estado.servidor_gv}, 
            		{:latido, Node.self(), estado.nv_conocido})
            	estado

            {:vista_tentativa, vista, valida} ->
            	estado = actualizar_estado_vt(estado,vista,valida)
            	if(Node.self() == estado.primario) do
            		if(estado.copia != :undefined) do
            			send({:servidor_sa, estado.copia},{:copia_backup, 
            				estado.bbdd, Node.self()})
	            		estado_copiar(estado)
            		end
            		bucle_recepcion_primario(estado)
            	end
            	estado


            {:copia_escribe, param, nodo_origen} ->
           		estado = procesar_escritura_cp(estado,param)
           		send({:servidor_sa, nodo_origen},:ok_escritura)
           		IO.inspect(estado)
           		estado

            {:copia_backup, bbdd, nodo_origen} ->
            	estado = procesar_backup(estado, bbdd)
            	send({:servidor_sa, nodo_origen},:ok_backup)
            	IO.inspect(estado)
            	estado
            
            # Solicitudes de lectura y escritura de clientes del servicio almace
            {_op, _param, _nodo_origen}  ->
	            send({:cliente_sa, estado.servidor_gv}, {:resultado, 
	            	:no_soy_primario_valido})
	            estado

	        _otro ->
	        	estado
        end

        bucle_recepcion_copia(estado)
    end

    #ESTADO ESCRIBIR

    defp estado_escribir(estado, param, nodo_cliente) do

    	receive do

            :ok_escritura -> 
            	procesar_escritura_ppl(estado,param, nodo_cliente)

            after @intervalo_latido->  
            	estado
        end

    end

    #ESTADO COPIAR

    defp estado_copiar(estado) do

    	receive do

            :ok_backup -> 
            	estado

            after @intervalo_latido->  
            	estado
        end
    end

    #FUNCIONES AUXILIARES

    defp actualizar_estado_vt(estado,vista,valida) do
    	if(valida == true) do
    		%{estado | primario: vista.primario, copia: vista.copia, 
    			nv_conocido: vista.num_vista}
    	else
    		%{estado | primario: :undefined, copia: :undefined,	nv_conocido: 0}
    	end
    end

    defp procesar_lectura(estado, param, nodo_origen) do
    	result = Map.get(estado.bbdd,param)
        result = if result == nil do "" else result end
        send({:cliente_sa, nodo_origen}, {:resultado, result})
    end

    defp procesar_escritura_ppl(estado, param,nodo_origen) do
    	result = Map.get(estado.bbdd,elem(param,0))
    	result = if result == nil do "" else result end
    	estado = %{estado | bbdd: Map.update(estado.bbdd,elem(param,0),
    		elem(param,1), fn(_) -> elem(param,1) end)}

    	send({:cliente_sa, nodo_origen}, {:resultado, result})
    	IO.inspect(estado)
    	estado
        
    end

    defp procesar_escritura_cp(estado, param) do
        %{estado | bbdd: Map.update(estado.bbdd,elem(param,0),elem(param,1),
        	fn(_) -> elem(param,1) end)}
    end

    defp procesar_backup(estado, bbdd) do
        %{estado | bbdd: Map.new(bbdd)}
    end

end