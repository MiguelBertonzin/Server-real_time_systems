# M3 — Servidor periódico para a NAV_PLAN

A adaptação usa o evento `NAV` da M2. Cada pedido avança a rota do drone para o próximo waypoint. Partindo do waypoint 0, a sequência é 1, 2, 0 e se repete. Os valores de roll, pitch e yaw da M2 estão em `nav_plan.c`.

## Compilar e executar

No GitHub, abra **Code → Codespaces → Create codespace on main**. No terminal Linux do Codespace ou do Ubuntu no WSL, compile e execute com os comandos abaixo.

```bash
make clean
make
./servidor_m3
```

A primeira execução usa `SCHED_OTHER`. Use o comando abaixo para executar o servidor com `SCHED_FIFO` e prioridade 60.

```bash
sudo ./servidor_m3 --rt
```

A primeira linha da tabela mostra a política e a prioridade efetivas da thread. Se o Codespace retornar `Operation not permitted` no modo `--rt`, use o Ubuntu WSL para demonstrar `SCHED_FIFO`. O projeto usa C11, GCC, Make e pthreads.

## Fila e medições

- A thread principal enfileira dez pedidos em instantes predefinidos. O mutex protege a fila FIFO.
- A thread do servidor abre uma janela a cada **50 ms** (`Ts`), com **20 ms** de orçamento (`Cs`). Ela usa `CLOCK_MONOTONIC` e espera absoluta com `clock_nanosleep`.
- Cada pedido repete a carga aritmética da M2 por cerca de **4 ms de CPU** antes de atualizar o waypoint. O servidor mede o consumo com `CLOCK_THREAD_CPUTIME_ID` e exige pelo menos **5 ms** de folga no orçamento e na janela antes de iniciar outro pedido. Essa margem é uma estimativa para o ensaio.
- O tempo de resposta vai da entrada na fila até o fim do atendimento. O prazo continua em **20 ms**, como na M2. Alguns pedidos da rajada ficam para a janela seguinte. Confira a coluna `Janela` para observar o efeito no tempo de resposta.

O servidor usa como referência o exemplo do professor. [Aperiodic_Server_ex.c — Felipe Viel](https://github.com/VielF/Real_Time_Systems/blob/main/Codes/Aperiodic_Server_ex.c)
