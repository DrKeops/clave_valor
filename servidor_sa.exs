Code.require_file("#{__DIR__}/debug.exs")

defmodule ServidorSA do
                
    defstruct primario: :undefined, copia: :undefined, servidor_gv: :undefined, 
    			nv_conocido: 0, bbdd: Map.new(), duplicados: Map.new(), 
    			esperando_copiar: false


    @intervalo_latido 50

    @depuracion false

    defp struct_inicial() do
        %ServidorSA{}
    end

    @doc """
        Obtener el hash de un string Elixir
            - Necesario pasar, previamente,  a formato string Erlang
         - Devuelve entero
    """
    def hash(string_concatenado) do
        String.to_charlist(string_concatenado) |> :erlang.phash2 |> 
        	Integer.to_string
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
            	if(valida == false) do
            		bucle_recepcion_principal(%{estado | nv_conocido: 0})
            	end
            	if(Node.self() == estado.primario) do
            		bucle_recepcion_primario(estado)
            	end
            	if(Node.self() == estado.copia) do
            		bucle_recepcion_copia(estado)
            	end
            	estado

            {:copia_escribe, _param, nodo_origen} ->
           		send({:cliente_sa, nodo_origen}, :no_soy_copia_valido)
            	estado

            # Solicitudes de lectura y escritura de clientes del servicio almace
            {_op, _num_op, _param, nodo_origen}  ->
            	send({:cliente_sa, nodo_origen}, {:resultado, 
            		:no_soy_primario_valido})
            	estado

	        _otro ->
	        	estado
        end

        bucle_recepcion_principal(estado)
    end

    #ESTADO PRIMARIO

    defp bucle_recepcion_primario(estado) do

    	backup? = not estado.esperando_copiar
        estado = receive do

        	:envia_latido ->
            	send({:servidor_gv, estado.servidor_gv}, 
            		{:latido, Node.self(), estado.nv_conocido})
            	estado

            {:vista_tentativa, vista, valida} ->
            	copia_ant = estado.copia
            	estado = actualizar_estado_vt(estado,vista,valida)
            	if(valida == false) do
            		bucle_recepcion_principal(%{estado | nv_conocido: 0})
            	end
            	if((estado.copia != copia_ant || 
            		estado.esperando_copiar == true ) &&
            		 estado.copia != :undefined) do

	            	estado_copiar( %{estado | esperando_copiar: true})
	            else estado
	            end
	            

            # Solicitudes de lectura y escritura de clientes del servicio almace
            {op, num_op, param, nodo_origen} when (backup?) ->
            	value = Map.get(estado.duplicados,nodo_origen)
            	if(value != nil && elem(value,0) == num_op) do
            		send({:cliente_sa, nodo_origen},{:resultado, elem(value,1)})
            		if @depuracion do Debug.msg("duplicado",elem(value,1)) end
            		estado
            	else
            		estado = if(op == :lee) do
            			procesar_lectura(estado, num_op, param, nodo_origen)
            		else estado
            		end
            		if(op == :escribe_generico) do
            			estado_escribir(estado, num_op, param, nodo_origen)
            		else estado	
            		end
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
            	if(valida == false) do
            		bucle_recepcion_principal(%{estado | nv_conocido: 0})
            	end
            	if(Node.self() == estado.primario) do
            		estado = if(estado.copia != :undefined) do
	            		estado_copiar( %{estado | esperando_copiar: true})
	            	else estado
            		end
            		bucle_recepcion_primario(estado)
            	else estado
            	end


            {:copia_escribe, num_op, nodo_cliente, param, nodo_origen} ->
           		estado = procesar_escritura(estado,num_op,param,nodo_cliente,
           			:cp)
           		send({:servidor_sa, nodo_origen},:ok_escritura)
           		if @depuracion do Debug.msg("copia_ecribe",estado) end
           		estado

            {:copia_backup, bbdd, duplicados, nodo_origen} ->
            	estado = procesar_backup(estado, bbdd, duplicados)
            	send({:servidor_sa, nodo_origen},:ok_backup)
            	if @depuracion do Debug.msg("backup", estado) end
            	estado
            
            # Solicitudes de lectura y escritura de clientes del servicio almace
            {_op, _num_op, _param, nodo_origen}  ->
	            send({:cliente_sa, nodo_origen}, {:resultado, 
	            	:no_soy_primario_valido})
	            estado

	        _otro ->
	        	estado
        end

        bucle_recepcion_copia(estado)
    end

    #ESTADO ESCRIBIR

    defp estado_escribir(estado, num_op, param, nodo_cliente) do

    	send({:servidor_sa, estado.copia},{:copia_escribe, num_op, nodo_cliente,
    		param, Node.self()})

    	receive do
            :ok_escritura -> 
            	estado = procesar_escritura(estado, num_op, param, 
            		nodo_cliente, :ppl)
            	if @depuracion do Debug.msg("primario_escribe",estado) end
            	estado

            :no_soy_copia_valido -> 
				estado         	

            after @intervalo_latido->  
            	estado
        end
    end

    #ESTADO COPIAR

    defp estado_copiar(estado) do

    	send({:servidor_sa, estado.copia},{:copia_backup, estado.bbdd, 
    		estado.duplicados, Node.self()})

    	receive do
            :ok_backup -> 
            	%{estado | esperando_copiar: false}

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

    defp procesar_lectura(estado, num_op, param, nodo_origen) do
    	result = Map.get(estado.bbdd,param)
        result = if result == nil do "" else result end
        send({:cliente_sa, nodo_origen}, {:resultado, result})
        estado = %{estado | duplicados: Map.update(estado.duplicados,
        	nodo_origen, {num_op, result}, fn(_) -> {num_op, result} end)}
        if @depuracion do Debug.msg("primario_lee",estado) end
        estado
    end

    defp procesar_escritura(estado, num_op, param, nodo_origen, flag) do
    	{estado, result} = if(elem(param,2) == false) do
    		{%{estado | bbdd: Map.update(estado.bbdd,elem(param,0),
    			elem(param,1), fn(_) -> elem(param,1) end)}, elem(param,1)}
    		
    	else
    		result = Map.get(estado.bbdd,elem(param,0))
    		result = if result == nil do "" else result end
    		{%{estado | bbdd: Map.update(estado.bbdd,elem(param,0),
    			hash(result <> elem(param,1)), 
    			fn(_) -> hash(result <> elem(param,1)) end)}, result}
    	end
    	
    	if (flag == :ppl) do
			send({:cliente_sa, nodo_origen}, {:resultado, result})
		end
    	%{estado | duplicados: Map.update(estado.duplicados,nodo_origen,
        	{num_op, result}, fn(_) -> {num_op, result} end)}        
    end

    defp procesar_backup(estado, bbdd, duplicados) do
        %{estado | bbdd: Map.new(bbdd), duplicados: Map.new(duplicados)}
    end

end
