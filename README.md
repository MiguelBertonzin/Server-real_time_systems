# M3 — Servidor periódico para a NAV_PLAN

A adaptação usa o evento `NAV` da M2. Cada pedido avança a rota do drone para o próximo waypoint. Partindo do waypoint 0, a sequência é 1, 2, 0 e se repete. Os valores de roll, pitch e yaw da M2 estão em `nav_plan.c`.

## Compilar e executar

No GitHub, abra **Code → Codespaces → Create codespace on main**. No terminal do Codespace ou do Ubuntu no WSL, rode os comandos na pasta que contém o `Makefile`.

```bash
make clean
make
./servidor_m3
```

A primeira execução usa `SCHED_OTHER`. Com `--rt`, o servidor usa `SCHED_FIFO` com prioridade 60 e as tarefas periódicas usam prioridade 50.

```bash
sudo ./servidor_m3 --rt
```

A primeira linha da saída mostra a política e a prioridade efetivas do servidor. Cada execução dura cerca de 10 segundos e imprime a tabela de resultados ao final.

No Windows, o PowerShell também pode chamar o Ubuntu WSL. Abra o PowerShell na pasta do projeto e execute este comando.

```powershell
wsl -d Ubuntu --exec bash -lc 'make clean && make && sudo ./servidor_m3 --rt'
```

## Fila e medições

- `T1`, `T2`, `T3` e `T4` são threads periódicas simuladas com períodos de **40, 80, 120 e 160 ms**, respectivamente. Cada ativação ocupa cerca de 5 ms de CPU.
- A thread principal começa com uma rajada e enfileira novos pedidos em instantes predefinidos durante o ensaio. O mutex protege a fila FIFO.
- A thread do servidor abre uma janela a cada **50 ms** (`Ts`), com **20 ms** de orçamento (`Cs`). Ela usa `CLOCK_MONOTONIC` e espera absoluta com `clock_nanosleep`.
- Cada pedido repete a carga aritmética da M2 por cerca de **4 ms de CPU** antes de atualizar o waypoint. O servidor mede o consumo com `CLOCK_THREAD_CPUTIME_ID` e exige pelo menos **5 ms** de folga no orçamento e na janela antes de iniciar outro pedido. Essa margem é uma estimativa para o ensaio.
- O terminal mostra quando cada tarefa inicia, quando o gerador põe um pedido na fila e em qual janela o servidor o atende. O tempo de resposta vai da entrada na fila até o fim do atendimento. O prazo continua em **20 ms**, como na M2.

O servidor usa como referência o exemplo do professor. [Aperiodic_Server_ex.c — Felipe Viel](https://github.com/VielF/Real_Time_Systems/blob/main/Codes/Aperiodic_Server_ex.c)
