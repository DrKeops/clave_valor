Code.require_file("#{__DIR__}/nodo_remoto.exs")
Code.require_file("#{__DIR__}/servidor_gv.exs")
Code.require_file("#{__DIR__}/cliente_gv.exs")
Code.require_file("#{__DIR__}/servidor_sa.exs")
Code.require_file("#{__DIR__}/cliente_sa.exs")

#Poner en marcha el servicio de tests unitarios con tiempo de vida limitada
# seed: 0 para que la ejecucion de tests no tenga orden aleatorio
ExUnit.start([timeout: 30000, seed: 0, exclude: [:no_ejecutar]]) # milisegundos

defmodule  ServicioAlmacenamientoTest do

    use ExUnit.Case

    # @moduletag timeout 100  para timeouts de todos lo test de este modulo

    @host0 "127.0.0.1"
    #@host0 "155.210.154.192"
    @host1 "127.0.0.1"
    #@host1 "155.210.154.193"
    @host2 "127.0.0.1"
    #@host2 "155.210.154.194"
    @host3 "127.0.0.1"
    #@host3 "155.210.154.195"


    @latidos_fallidos 4

    @intervalo_latido 50


    #setup_all do
    

    @tag :no_ejecutar
    test "solo_arranque y parada" do
        IO.puts("Test: Solo arranque y parada ...")
        # Poner en marcha nodos
        gv = ServidorGV.start(@host1, "gv")
        sa1 = ServidorSA.start(@host3, "sa1", gv)
        ca1 = ClienteSA.start(@host2, "ca1", gv)

        Process.sleep(300)

        # Parar todos los nodos
        parar_nodos([ca1, sa1, gv])
        IO.puts(" ... Superado")
    end

    @tag :no_ejecutar
    test "Algunas escrituras" do
        #:io.format "Pids de nodo MAESTRO ~p: principal = ~p~n", [node, self]

        #Process.flag(:trap_exit, true)

        # Para que funcione bien la función  ClienteGV.obten_vista
        Process.register(self(), :servidor_sa)

        # Arrancar nodos : 1 GV, 3 servidores y 1 cliente de almacenamiento
        # Lista de tupas con informacion para crear diferentes nodos
        lista_datos_nodos = [gv: [@host0, "gv"], ca: [@host0, "ca1"],
                  sa: [@host1, "sa1"], sa: [@host2, "sa2"], sa: [@host3, "sa3"]]
        mapa_nodos = arrancar_nodos(lista_datos_nodos)

        # Espera configuracion y relacion entre nodos
        Process.sleep(200)

        IO.puts("Test: Comprobar escritura con primario, copia y espera ...")

        # Comprobar primeros nodos primario y copia
        {%{primario: p, copia: c}, _ok} = ClienteGV.obten_vista(mapa_nodos.gv)       
        assert p == mapa_nodos.sa1
        assert c == mapa_nodos.sa2

        ClienteSA.escribe(mapa_nodos.ca1, "a", "aa", 0)
        ClienteSA.escribe(mapa_nodos.ca1, "b", "bb", 0)
        ClienteSA.escribe(mapa_nodos.ca1, "c", "cc", 0)
        comprobar(mapa_nodos.ca1, "a", "aa")
        comprobar(mapa_nodos.ca1, "b", "bb")
        comprobar(mapa_nodos.ca1, "c", "cc")

        IO.puts(" ... Superado")
       
        IO.puts("Test: Comprobar escritura despues de fallo de nodo copia ...")

        # Provocar fallo de nodo copia y seguido realizar una escritura
        {%{copia: nodo_copia},_} = ClienteGV.obten_vista(mapa_nodos.gv)
        NodoRemoto.stop(nodo_copia) # Provocar parada nodo copia

        Process.sleep(700) # esperar a reconfiguracion de servidores

        ClienteSA.escribe(mapa_nodos.ca1, "a", "aaa", 0)
        comprobar(mapa_nodos.ca1, "a", "aaa")

        # Comprobar los nuevo nodos primario y copia
        {%{primario: p, copia: c}, _ok} = ClienteGV.obten_vista(mapa_nodos.gv)       
        assert p == mapa_nodos.sa1
        assert c == mapa_nodos.sa3

        #Para que se muestren correctamente los mensajes de depuracion
        Process.sleep(200)

        IO.puts(" ... Superado")

        # Parar todos los nodos
        parar_nodos(mapa_nodos)
    end

    @tag :no_ejecutar
    test "Mismos valores concurrentes" do
        IO.puts("Test: Escrituras mismos valores clientes concurrentes ...")

        # Para que funcione bien la función  ClienteGV.obten_vista
        Process.register(self(), :servidor_sa)

        # Arrancar nodos : 1 GV, 3 servidores y 3 cliente de almacenamiento
        # Lista de tupas con informacion para crear diferentes nodos
        lista_datos_nodos = [gv: [@host0, "gv"],
                  sa: [@host1, "sa1"], sa: [@host2, "sa2"], sa: [@host3, "sa3"],
                  ca: [@host0, "ca1"], ca: [@host0, "ca2"], ca: [@host0, "ca3"]]
        mapa_nodos = arrancar_nodos(lista_datos_nodos)

        # Espera configuracion y relacion entre nodos
        Process.sleep(200)

        # Comprobar primeros nodos primario y copia
        {%{primario: p, copia: c}, _ok} = ClienteGV.obten_vista(mapa_nodos.gv)       
        assert p == mapa_nodos.sa1
        assert c == mapa_nodos.sa2

        # Escritura concurrente de mismas 2 claves, pero valores diferentes
        # Posteriormente comprobar que estan igual en primario y copia
        escritura_concurrente(mapa_nodos)

        Process.sleep(200)

        #Obtener valor de las clave "0" y "1" con el primer primario
         valor1primario = ClienteSA.lee(mapa_nodos.ca1, "0")
         valor2primario = ClienteSA.lee(mapa_nodos.ca3, "1")

         # Forzar parada de primario
         NodoRemoto.stop(ClienteGV.primario(mapa_nodos.gv))

        # Esperar detección fallo y reconfiguración copia a primario
        Process.sleep(1000)

        # Obtener valor de clave "0" y "1"con segundo primario (copia anterior)
         valor1copia = ClienteSA.lee(mapa_nodos.ca3, "0")
         valor2copia = ClienteSA.lee(mapa_nodos.ca2, "1")

        IO.puts"valor1primario = #{valor1primario},valor1copia = #{valor1copia}"
           <> "valor2primario = #{valor2primario},valor2copia = #{valor2copia}"
        # Verificar valores obtenidos con primario y copia inicial
        assert valor1primario == valor1copia
        assert valor2primario == valor2copia

        # Parar todos los nodos
        parar_nodos(mapa_nodos)

         IO.puts(" ... Superado")
    end

    # 1 Parada de todos los servidores de almacenamiento y adición de uno nuevo 
    # que no debería estar activo...
    @tag :no_ejecutar
    test "arranque y parada, servidor nuevo" do
        IO.puts("Test: Solo arranque y parada ...")
        # Poner en marcha nodos
        gv = ServidorGV.start(@host1, "gv")
        sa1 = ServidorSA.start(@host3, "sa1", gv)
        sa2 = ServidorSA.start(@host3, "sa2", gv)
        ca1 = ClienteSA.start(@host2, "ca1", gv)

        Process.sleep(300)

        # Parar todos los nodos
        parar_nodos([ca1, sa1, sa2])

        Process.sleep(300)

        sa3 = ServidorSA.start(@host3, "sa3", gv)

        Process.register(self(), :cliente_sa)

        send({:servidor_sa, sa3}, {:lee, 0, -1, Node.self()})
        receive do
            {:resultado, valor} -> 
            	assert valor == :no_soy_primario_valido
        end

        

        IO.puts(" ... Superado")

        parar_nodos([sa3, gv])
    end

    # 2. Petición de escritura duplicada por perdida de respuesta (modificación 
    # realizada en BD), con primario y copia.
    @tag :no_ejecutar
    test "escritura duplicada" do
        IO.puts("Test: Escritura duplicada ...")
        Process.register(self(), :servidor_sa)

        # Arrancar nodos : 1 GV, 3 servidores y 1 cliente de almacenamiento
        # Lista de tupas con informacion para crear diferentes nodos
        lista_datos_nodos = [gv: [@host0, "gv"], ca: [@host0, "ca1"],
                  sa: [@host1, "sa1"], sa: [@host2, "sa2"], sa: [@host3, "sa3"]]
        mapa_nodos = arrancar_nodos(lista_datos_nodos)

        # Espera configuracion y relacion entre nodos
        Process.sleep(200)

        # Comprobar primeros nodos primario y copia
        {%{primario: p, copia: c}, _ok} = ClienteGV.obten_vista(mapa_nodos.gv)       
        assert p == mapa_nodos.sa1
        assert c == mapa_nodos.sa2

        val_ant_a = ClienteSA.escribe_hash(mapa_nodos.ca1, "a", "aa", 1)
        val_ant_b = ClienteSA.escribe_hash(mapa_nodos.ca1, "b", "bb", 2)
        val_ant_c = ClienteSA.escribe_hash(mapa_nodos.ca1, "c", "cc", 3)
        comprobar(mapa_nodos.ca1, "a", siguiente_valor(val_ant_a,"aa"))
        comprobar(mapa_nodos.ca1, "b", siguiente_valor(val_ant_b,"bb"))
        comprobar(mapa_nodos.ca1, "c", siguiente_valor(val_ant_c,"cc"))

        IO.puts(" ... Superado")

        # Parar todos los nodos
        parar_nodos(mapa_nodos)
        
    end

    # 3. Petición de escritura inmediatamente después de la caída de nodo copia 
    # (con uno en espera que le reemplace).
	@tag :no_ejecutar
    test "escritura y cae copia" do
        IO.puts("Test: Escritura y cae copia ...")
        Process.register(self(), :servidor_sa)

        # Arrancar nodos : 1 GV, 3 servidores y 1 cliente de almacenamiento
        # Lista de tupas con informacion para crear diferentes nodos
        lista_datos_nodos = [gv: [@host0, "gv"], ca: [@host0, "ca1"],
                  sa: [@host1, "sa1"], sa: [@host2, "sa2"], sa: [@host3, "sa3"]]
        mapa_nodos = arrancar_nodos(lista_datos_nodos)

        # Espera configuracion y relacion entre nodos
        Process.sleep(200)

        # Comprobar primeros nodos primario y copia
        {%{primario: p, copia: c}, _ok} = ClienteGV.obten_vista(mapa_nodos.gv)       
        assert p == mapa_nodos.sa1
        assert c == mapa_nodos.sa2

        NodoRemoto.stop(mapa_nodos.sa2)
        ClienteSA.escribe(mapa_nodos.ca1, "a", "aa", 0)

        Process.sleep(300)

        # Comprobar primeros nodos primario y copia
        {%{primario: p, copia: c}, _ok} = ClienteGV.obten_vista(mapa_nodos.gv)       
        assert p == mapa_nodos.sa1
        assert c == mapa_nodos.sa3

        comprobar(mapa_nodos.ca1, "a", "aa")

        IO.puts(" ... Superado")

        # Parar todos los nodos
        parar_nodos(mapa_nodos)
        
    end

    # 4. Escrituras concurrentes de varios clientes sobre la misma clave, con 
    # comunicación con fallos (sobre todo pérdida de repuestas para comprobar 
    # gestión correcta de duplicados).
    @tag :no_ejecutar
    test "Mismos valores concurrentes con fallos" do
        IO.puts("Test: Escrituras mismos valores clientes concurrentes (dup)...")

        # Para que funcione bien la función  ClienteGV.obten_vista
        Process.register(self(), :servidor_sa)

        # Arrancar nodos : 1 GV, 3 servidores y 3 cliente de almacenamiento
        # Lista de tupas con informacion para crear diferentes nodos
        lista_datos_nodos = [gv: [@host0, "gv"],
                  sa: [@host1, "sa1"], sa: [@host2, "sa2"], sa: [@host3, "sa3"],
                  ca: [@host0, "ca1"], ca: [@host0, "ca2"], ca: [@host0, "ca3"]]
        mapa_nodos = arrancar_nodos(lista_datos_nodos)

        # Espera configuracion y relacion entre nodos
        Process.sleep(200)

        # Comprobar primeros nodos primario y copia
        {%{primario: p, copia: c}, _ok} = ClienteGV.obten_vista(mapa_nodos.gv)       
        assert p == mapa_nodos.sa1
        assert c == mapa_nodos.sa2

        # Escritura concurrente de mismas 2 claves, pero valores diferentes
        # Posteriormente comprobar que estan igual en primario y copia
        escritura_concurrente_dup(mapa_nodos)

        Process.sleep(200)

        #Obtener valor de las clave "0" y "1" con el primer primario
         valor1primario = ClienteSA.lee(mapa_nodos.ca1, "0")
         valor2primario = ClienteSA.lee(mapa_nodos.ca3, "1")

         # Forzar parada de primario
         NodoRemoto.stop(ClienteGV.primario(mapa_nodos.gv))

        # Esperar detección fallo y reconfiguración copia a primario
        Process.sleep(1000)

        # Obtener valor de clave "0" y "1"con segundo primario (copia anterior)
         valor1copia = ClienteSA.lee(mapa_nodos.ca3, "0")
         valor2copia = ClienteSA.lee(mapa_nodos.ca2, "1")

        IO.puts"valor1primario = #{valor1primario},valor1copia = #{valor1copia}"
           <> "valor2primario = #{valor2primario},valor2copia = #{valor2copia}"
        # Verificar valores obtenidos con primario y copia inicial
        assert valor1primario == valor1copia
        assert valor2primario == valor2copia

        # Parar todos los nodos
        parar_nodos(mapa_nodos)

         IO.puts(" ... Superado")
    end

    # 5. Comprobación de que un antiguo primario no debería servir operaciones 
    # de lectura.
    #@tag :no_ejecutar
    test "antiguo primario no lee" do
        IO.puts("Test: Antiguo primario no lee ...")
        Process.register(self(), :cliente_sa)

        # Arrancar nodos : 1 GV, 3 servidores y 1 cliente de almacenamiento
        # Lista de tupas con informacion para crear diferentes nodos
        lista_datos_nodos = [gv: [@host0, "gv"], ca: [@host0, "ca1"],
                  sa: [@host1, "sa1"], sa: [@host2, "sa2"], sa: [@host3, "sa3"]]
        mapa_nodos = arrancar_nodos(lista_datos_nodos)


        # Espera configuracion y relacion entre nodos
        Process.sleep(200)

        ClienteSA.escribe(mapa_nodos.ca1, "a", "aa", 0)
        NodoRemoto.stop(mapa_nodos.sa1)

        Process.sleep(200)

        ServidorSA.start(@host1, "sa1", mapa_nodos.gv)

        Process.sleep(100)

        ClienteSA.escribe(mapa_nodos.ca1, "b", "bb", 0)

        #Petición lectura a primario
        send({:servidor_sa, mapa_nodos.sa1}, {:lee, 0, -1, Node.self()})
        receive do
            {:resultado, valor} -> 
            	assert valor == :no_soy_primario_valido
        end

        IO.puts(" ... Superado")

        # Parar todos los nodos
        parar_nodos(mapa_nodos)
        
    end

    # ------------------ FUNCIONES DE APOYO A TESTS ------------------------

    defp comprobar(nodo_cliente, clave, valor_a_comprobar) do
        valor_en_almacen = ClienteSA.lee(nodo_cliente, clave)

        assert valor_en_almacen == valor_a_comprobar       
    end


    def siguiente_valor(previo, actual) do
        ServidorSA.hash(previo <> actual)
    end


    # Ejecutar durante un tiempo una escritura continuada de 2 clientes sobre
    # las mismas 2 claves pero con 3 valores diferentes de forma concurrente
    def escritura_concurrente(mapa_nodos) do
        aleat1 = :rand.uniform(1000)
        pid1 =spawn(__MODULE__, :bucle_infinito, [mapa_nodos.ca1, aleat1])
        aleat2 = :rand.uniform(1000)
        pid2 =spawn(__MODULE__, :bucle_infinito, [mapa_nodos.ca2, aleat2])
        aleat3 = :rand.uniform(1000)
        pid3 =spawn(__MODULE__, :bucle_infinito, [mapa_nodos.ca3, aleat3])

        Process.sleep(3000)

        Process.exit(pid1, :kill)
        Process.exit(pid2, :kill)
        Process.exit(pid3, :kill)
    end

    def bucle_infinito(nodo_cliente, aleat) do
        clave = Integer.to_string(rem(aleat, 2)) # solo claves "1" y "2"
        valor = Integer.to_string(aleat)
        miliseg = :rand.uniform(300)

        Process.sleep(400)

        :io.format "Proceso ~p, escribe clave = ~p, valor = ~p, sleep = ~p~n",
                    [self, clave, valor, miliseg]

        ClienteSA.escribe(nodo_cliente, clave, valor, 0)

        bucle_infinito(nodo_cliente, aleat)
    end

    def escritura_concurrente_dup(mapa_nodos) do
        aleat1 = :rand.uniform(1000)
        pid1 =spawn(__MODULE__, :bucle_infinito_dup, [mapa_nodos.ca1, aleat1])
        aleat2 = :rand.uniform(1000)
        pid2 =spawn(__MODULE__, :bucle_infinito_dup, [mapa_nodos.ca2, aleat2])
        aleat3 = :rand.uniform(1000)
        pid3 =spawn(__MODULE__, :bucle_infinito_dup, [mapa_nodos.ca3, aleat3])

        Process.sleep(3000)

        Process.exit(pid1, :kill)
        Process.exit(pid2, :kill)
        Process.exit(pid3, :kill)
    end

    def bucle_infinito_dup(nodo_cliente, aleat) do
        clave = Integer.to_string(rem(aleat, 2)) # solo claves "1" y "2"
        valor = Integer.to_string(aleat)
        miliseg = :rand.uniform(300)
        n_fallos = :rand.uniform(3)

        Process.sleep(400)

        :io.format "Proceso ~p, escribe clave = ~p, valor = ~p, sleep = ~p, 
n_fallos ~p~n", [self, clave, valor, miliseg, n_fallos]

        ClienteSA.escribe_hash(nodo_cliente, clave, valor, n_fallos)

        bucle_infinito_dup(nodo_cliente, aleat)
    end


    # Lista de datos nodos, cada elemento es {clave, valor}:
    #   - Claves (tipo nodo): :gv, :sa o :ca
    #   - Valores : {host :: atomo), nombre ::String.t)}, para de cada nodo
    # el de un solo parametro solo se deberia invocar si hay datos dentro del GV
    defp arrancar_nodos(lista_datos_nodos) do
        # Resultados se guardan en otro mapa
        m_nodo_gv = for {:gv, [h, n]} <- lista_datos_nodos, into: %{} do
            {:gv, ServidorGV.start(h, n)}

        end

        arrancar_nodos(lista_datos_nodos, m_nodo_gv.gv) |> Map.merge(m_nodo_gv)
    end # sin datos de gestor de vistas
    defp arrancar_nodos(lista_datos_nodos, gv) do
        m_nodos_sas = for {:sa, [h, n]} <- lista_datos_nodos, into: %{} do
            {String.to_atom(n), ServidorSA.start(h, n, gv)}
        end

        m_nodos_cas = for {:ca, [h, n]} <- lista_datos_nodos, into: %{} do
            {String.to_atom(n), ClienteSA.start(h, n, gv)}
        end
          Map.merge(m_nodos_sas, m_nodos_cas)     
    end

    defp parar_nodos(lista_nodos) when is_list(lista_nodos) do
        Enum.each(lista_nodos, fn(nodo) -> NodoRemoto.stop(nodo) end)
    end
    defp parar_nodos(mapa_nodos) when is_map(mapa_nodos) do
        Enum.each(mapa_nodos, fn({_clave, nodo}) ->
                                    NodoRemoto.stop(nodo)
                               end)
    end

end