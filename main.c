#define _POSIX_C_SOURCE 200809L

#include "nav_plan.h"

#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define NS_POR_MS INT64_C(1000000)
#define NS_POR_S INT64_C(1000000000)

enum {
    DURACAO_S = 10,
    PERIODO_MS = 50,
    ORCAMENTO_MS = 20,
    RESERVA_NAV_MS = 5,
    CARGA_NAV_MS = 4,
    CARGA_TAREFA_MS = 5,
    PRAZO_NAV_MS = 20,
    PRIORIDADE_SERVIDOR = 60,
    PRIORIDADE_TAREFA = 50
};

// Rajada para encher a fila e pedidos espaçados até o fim da execução.
static const int chegadas_ms[] = {
    5, 8, 11, 14, 17, 41, 44, 75, 144, 193,
    443, 794, 1143, 1463, 1794, 2143, 2463, 2794,
    3143, 3463, 3794, 4143, 4463, 4794, 5143, 5463, 5794,
    6143, 6463, 6794, 7143, 7463, 7794, 8143, 8463, 8794,
    9143, 9463, 9794
};
enum { TOTAL_PEDIDOS = sizeof(chegadas_ms) / sizeof(chegadas_ms[0]) };

typedef struct {
    unsigned int id;
    int64_t chegada;
} Pedido;

typedef struct {
    Pedido pedido;
    Waypoint ponto;
    unsigned int waypoint;
    unsigned int janela;
    int64_t inicio;
    int64_t fim;
    int64_t cpu;
} Resultado;

typedef struct {
    pthread_mutex_t mutex;
    Pedido fila[TOTAL_PEDIDOS];
    size_t primeiro;
    size_t proximo;
    Resultado resultados[TOTAL_PEDIDOS];
    int64_t inicio;
    int64_t fim;
} Execucao;

typedef struct {
    const char *nome;
    int periodo_ms;
    const Execucao *execucao;
} Tarefa;

static double em_ms(int64_t tempo)
{
    return (double)tempo / NS_POR_MS;
}

static int64_t tempo_ns(clockid_t relogio)
{
    struct timespec tempo;
    if (clock_gettime(relogio, &tempo) != 0) {
        perror("clock_gettime");
        exit(EXIT_FAILURE);
    }
    return (int64_t)tempo.tv_sec * NS_POR_S + tempo.tv_nsec;
}

static void dormir_ate(int64_t instante)
{
    struct timespec tempo = {
        .tv_sec = instante / NS_POR_S,
        .tv_nsec = instante % NS_POR_S
    };
    int erro;
    do {
        erro = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &tempo, NULL);
    } while (erro == EINTR);
    if (erro != 0) {
        fprintf(stderr, "clock_nanosleep: %s\n", strerror(erro));
        exit(EXIT_FAILURE);
    }
}

static void enfileirar(Execucao *execucao, unsigned int id)
{
    pthread_mutex_lock(&execucao->mutex);
    Pedido pedido = {
        .id = id,
        .chegada = tempo_ns(CLOCK_MONOTONIC)
    };
    execucao->fila[execucao->proximo++] = pedido;
    pthread_mutex_unlock(&execucao->mutex);
    printf("[%8.2f ms] [GERADOR] pedido NAV %u enfileirado\n",
           em_ms(pedido.chegada - execucao->inicio), id);
}

static int retirar(Execucao *execucao, Pedido *pedido)
{
    pthread_mutex_lock(&execucao->mutex);
    int tem_pedido = execucao->primeiro < execucao->proximo;
    if (tem_pedido)
        *pedido = execucao->fila[execucao->primeiro++];
    pthread_mutex_unlock(&execucao->mutex);
    return tem_pedido;
}

static void ocupar_cpu(int duracao_ms, float semente)
{
    int64_t inicio = tempo_ns(CLOCK_THREAD_CPUTIME_ID);
    do {
        semente = nav_plan_calcular_carga(semente);
    } while (tempo_ns(CLOCK_THREAD_CPUTIME_ID) - inicio < duracao_ms * NS_POR_MS);
}

static void atender(Execucao *execucao, Pedido pedido, unsigned int janela,
                    unsigned int *indice_waypoint)
{
    Resultado *resultado = &execucao->resultados[pedido.id - 1];
    resultado->pedido = pedido;
    resultado->janela = janela;
    resultado->inicio = tempo_ns(CLOCK_MONOTONIC);

    int64_t cpu_inicio = tempo_ns(CLOCK_THREAD_CPUTIME_ID);
    ocupar_cpu(CARGA_NAV_MS, (float)pedido.id);
    resultado->ponto = nav_plan_avancar(indice_waypoint);
    resultado->waypoint = *indice_waypoint;
    resultado->cpu = tempo_ns(CLOCK_THREAD_CPUTIME_ID) - cpu_inicio;
    resultado->fim = tempo_ns(CLOCK_MONOTONIC);
    printf("[%8.2f ms] [SERVIDOR] janela %u | NAV %u -> waypoint %u\n",
           em_ms(resultado->fim - execucao->inicio), janela, pedido.id, resultado->waypoint);
}

static void *tarefa_periodica(void *arg)
{
    const Tarefa *tarefa = arg;
    const Execucao *execucao = tarefa->execucao;
    const int64_t periodo = tarefa->periodo_ms * NS_POR_MS;
    int64_t liberacao = execucao->inicio;

    while (liberacao < execucao->fim) {
        dormir_ate(liberacao);
        int64_t agora = tempo_ns(CLOCK_MONOTONIC);
        if (agora >= execucao->fim)
            break;

        printf("[%8.2f ms] [%s] executando\n",
               em_ms(agora - execucao->inicio), tarefa->nome);
        ocupar_cpu(CARGA_TAREFA_MS, (float)tarefa->periodo_ms);

        // Se houve atraso, segue a agenda em vez de repetir ativações vencidas.
        liberacao += ((agora - liberacao) / periodo + 1) * periodo;
    }
    return NULL;
}

static void *servidor(void *arg)
{
    Execucao *execucao = arg;
    unsigned int indice_waypoint = 0;
    int64_t liberacao = execucao->inicio;
    const int64_t periodo = PERIODO_MS * NS_POR_MS;
    const int64_t orcamento = ORCAMENTO_MS * NS_POR_MS;
    const int64_t reserva = RESERVA_NAV_MS * NS_POR_MS;

    while (liberacao < execucao->fim) {
        dormir_ate(liberacao);
        int64_t agora = tempo_ns(CLOCK_MONOTONIC);
        if (agora >= execucao->fim)
            break;

        liberacao += (agora - liberacao) / periodo * periodo;
        unsigned int janela = (unsigned int)((liberacao - execucao->inicio) / periodo);
        int64_t proxima = liberacao + periodo;
        int64_t cpu_inicio = tempo_ns(CLOCK_THREAD_CPUTIME_ID);

        // Só retira um pedido quando ainda cabe a reserva de atendimento.
        while (tempo_ns(CLOCK_THREAD_CPUTIME_ID) - cpu_inicio + reserva <= orcamento &&
               tempo_ns(CLOCK_MONOTONIC) + reserva <= proxima) {
            Pedido pedido;
            if (!retirar(execucao, &pedido))
                break;

            atender(execucao, pedido, janela, &indice_waypoint);
        }
        liberacao = proxima;
    }
    return NULL;
}

static int criar_thread(pthread_t *thread, void *(*funcao)(void *), void *arg,
                        bool tempo_real, int prioridade)
{
    pthread_attr_t attr;
    int erro = pthread_attr_init(&attr);
    if (erro != 0)
        return erro;

    int politica = tempo_real ? SCHED_FIFO : SCHED_OTHER;
    struct sched_param param = {
        .sched_priority = tempo_real ? prioridade : 0
    };
    erro = pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);
    if (erro == 0)
        erro = pthread_attr_setschedpolicy(&attr, politica);
    if (erro == 0)
        erro = pthread_attr_setschedparam(&attr, &param);
    if (erro == 0)
        erro = pthread_create(thread, &attr, funcao, arg);
    pthread_attr_destroy(&attr);
    return erro;
}

static void imprimir_resultados(const Execucao *execucao)
{
    puts("\nNAV_PLAN — Tempos em ms desde o início. Janela 0 começa em 0 ms.\n");
    puts(" ID  Janela  Chegada   Início      Fim    CPU  Resposta  Prazo    WP  (roll, pitch, yaw)");

    size_t no_prazo = 0;
    for (size_t i = 0; i < execucao->primeiro; ++i) {
        const Resultado *r = &execucao->resultados[i];
        int64_t resposta = r->fim - r->pedido.chegada;
        bool cumpriu = resposta <= PRAZO_NAV_MS * NS_POR_MS;
        no_prazo += cumpriu;
        printf("%3u %7u %8.2f %8.2f %8.2f %6.2f %9.2f  %-7s %2u  (%g, %g, %g)\n",
               r->pedido.id, r->janela, em_ms(r->pedido.chegada - execucao->inicio),
               em_ms(r->inicio - execucao->inicio), em_ms(r->fim - execucao->inicio),
               em_ms(r->cpu), em_ms(resposta), cumpriu ? "ok" : "perdido",
               r->waypoint, r->ponto.roll_deg, r->ponto.pitch_deg, r->ponto.yaw_deg);
    }
    printf("\n%zu/%zu atendidos no prazo, %zu fora do prazo, %zu ficaram na fila.\n",
           no_prazo, execucao->primeiro, execucao->primeiro - no_prazo,
           execucao->proximo - execucao->primeiro);
}

int main(int argc, char **argv)
{
    bool tempo_real = argc == 2 && strcmp(argv[1], "--rt") == 0;
    if (argc > 1 && !tempo_real) {
        fprintf(stderr, "Uso: %s [--rt]\n", argv[0]);
        return EXIT_FAILURE;
    }

    setvbuf(stdout, NULL, _IOLBF, BUFSIZ);
    int64_t inicio = tempo_ns(CLOCK_MONOTONIC) + 100 * NS_POR_MS;
    Execucao execucao = {
        .mutex = PTHREAD_MUTEX_INITIALIZER,
        .inicio = inicio,
        .fim = inicio + DURACAO_S * NS_POR_S
    };
    Tarefa tarefas[] = {
        {"T1", 40, &execucao},
        {"T2", 80, &execucao},
        {"T3", 120, &execucao},
        {"T4", 160, &execucao}
    };
    enum { TOTAL_TAREFAS = sizeof(tarefas) / sizeof(tarefas[0]) };
    pthread_t threads[1 + TOTAL_TAREFAS];

    int erro = criar_thread(&threads[0], servidor, &execucao, tempo_real, PRIORIDADE_SERVIDOR);
    if (erro != 0) {
        fprintf(stderr, "Falha ao criar o servidor com %s: %s\n",
                tempo_real ? "SCHED_FIFO" : "SCHED_OTHER", strerror(erro));
        return EXIT_FAILURE;
    }

    int politica;
    struct sched_param param;
    erro = pthread_getschedparam(threads[0], &politica, &param);
    if (erro != 0) {
        fprintf(stderr, "pthread_getschedparam: %s\n", strerror(erro));
        return EXIT_FAILURE;
    }
    printf("NAV_PLAN | %s prioridade %d | Ts=%d ms | Cs=%d ms | prazo=%d ms | duração=%d s\n\n",
           politica == SCHED_FIFO ? "SCHED_FIFO" : "SCHED_OTHER",
           param.sched_priority, PERIODO_MS, ORCAMENTO_MS, PRAZO_NAV_MS, DURACAO_S);

    for (size_t i = 0; i < TOTAL_TAREFAS; ++i) {
        erro = criar_thread(&threads[i + 1], tarefa_periodica, &tarefas[i],
                            tempo_real, PRIORIDADE_TAREFA);
        if (erro != 0) {
            fprintf(stderr, "Falha ao criar %s: %s\n", tarefas[i].nome, strerror(erro));
            return EXIT_FAILURE;
        }
    }

    for (unsigned int i = 0; i < TOTAL_PEDIDOS; ++i) {
        dormir_ate(execucao.inicio + chegadas_ms[i] * NS_POR_MS);
        if (tempo_ns(CLOCK_MONOTONIC) >= execucao.fim)
            break;
        enfileirar(&execucao, i + 1);
    }

    dormir_ate(execucao.fim);
    for (size_t i = 0; i < 1 + TOTAL_TAREFAS; ++i) {
        erro = pthread_join(threads[i], NULL);
        if (erro != 0) {
            fprintf(stderr, "pthread_join: %s\n", strerror(erro));
            return EXIT_FAILURE;
        }
    }
    pthread_mutex_destroy(&execucao.mutex);
    imprimir_resultados(&execucao);
    return EXIT_SUCCESS;
}
