import etcd3
import sys
import time
import threading
import msvcrt  # Só funciona no Windows

# Conecta ao etcd
# client() cria um cliente para interagir com um servidor etcd.
etcd = etcd3.client(host='localhost', port=2379)

# Nome do candidato a líder é passado como argumento na linha de comando
nome_candidato = sys.argv[1]
print(f'Candidato {nome_candidato}')

# Tenta se tornar líder
def tornar_se_lider():
    # Cria um lease de 10 segundos
    # lease() cria um novo lease. O lease é um contrato que deleta a chave após um período de tempo se o lease não for renovado.
    lease = etcd.lease(10)

    # Tenta colocar a chave 'lider' com o nome do candidato usando o lease
    # transaction() executa uma transação etcd, que é uma operação atômica.
    # put() coloca um valor em uma chave. Se a chave já existe, o valor é substituído.
    status, _ = etcd.transaction(
        compare=[etcd.transactions.version('lider') == 0], # Se não existir lider
        success=[etcd.transactions.put('lider', nome_candidato, lease.id)], # Adiciona o próprio candidato como lider
        failure=[]  # Senão ...
    )

    # Confirma se o candidato se tornou o líder
    # get() obtém o valor de uma chave.
    lider_atual, _ = etcd.get('lider')
    lider_atual = lider_atual.decode('utf-8')
    if lider_atual != nome_candidato:
        status = False

    return status, lease

# Renova a liderança
def renovar_lideranca(lease):
    while True:
        try:
            # Renova o lease
            # refresh() renova um lease, estendendo seu tempo de vida.
            lease.refresh()
            time.sleep(2)
            print("Liderança renovada, pressione qualquer tecla para parar a execução.")

            # Verifica se qualquer tecla foi pressionada
            if msvcrt.kbhit():
                print('É o fim para o líder...')
                sys.exit(0)

        except:
            # Se houver qualquer exceção, o candidato perdeu a liderança
            print(f'{nome_candidato} perdeu a liderança...')
            break

# Observa a chave 'lider' e tenta se tornar líder quando a chave é excluída
def observar_lider():
    while True:
        # watch() observa mudanças em uma chave ou intervalo de chaves.
        events_iterator, cancel = etcd.watch('lider')
        for event in events_iterator:
            if isinstance(event, etcd3.events.DeleteEvent):
                print(f'Líder está fora do ar...')
                print(f'{nome_candidato} está tentando a liderança...')
                status, lease = tornar_se_lider()
                if status:
                    print(f'{nome_candidato} se tornou o líder!')
                    renovar_lideranca(lease)
                    sys.exit(0)
                else:
                    lider_atual, _ = etcd.get('lider')
                    lider_atual = lider_atual.decode('utf-8')
                    print(f'{nome_candidato} não conseguiu se tornar o líder, {lider_atual} se tornou o líder.')

# Inicia a observação da chave 'lider' em uma nova thread
threading.Thread(target=observar_lider).start()

# Tentativa inicial de se tornar líder
status, lease = tornar_se_lider()
if status:
    print(f'{nome_candidato} se tornou o líder!')
    # O candidato agora é o líder, então ele precisa renovar seu lease para manter a liderança
    renovar_lideranca(lease)
    # Se o candidato se tornou o líder e renovou seu lease, podemos terminar o programa
    sys.exit(0)
else:
    # Se o candidato não conseguiu se tornar o líder, ele verifica quem é o líder atual
    lider_atual, _ = etcd.get('lider')
    lider_atual = lider_atual.decode('utf-8')
    print(f'{nome_candidato} não é o líder, {lider_atual} é o líder.')
