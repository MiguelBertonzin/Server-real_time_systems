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
    PERIODO_MS = 50,
    ORCAMENTO_MS = 20,
    RESERVA_NAV_MS = 5,
    CARGA_NAV_MS = 4,
    PRAZO_NAV_MS = 20,
    PRIORIDADE_SERVIDOR = 60
};

// Rajada para exercitar a fila, seguida de pedidos próximos da próxima janela.
static const int chegadas_ms[] = {5, 8, 11, 14, 17, 41, 44, 75, 144, 193};
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
    int politica;
    int prioridade;
} Execucao;

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
    execucao->fila[execucao->proximo++] = (Pedido){
        .id = id,
        .chegada = tempo_ns(CLOCK_MONOTONIC)
    };
    pthread_mutex_unlock(&execucao->mutex);
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

static void atender(Execucao *execucao, Pedido pedido, unsigned int janela,
                    unsigned int *indice_waypoint)
{
    Resultado *resultado = &execucao->resultados[pedido.id - 1];
    resultado->pedido = pedido;
    resultado->janela = janela;
    resultado->inicio = tempo_ns(CLOCK_MONOTONIC);

    int64_t cpu_inicio = tempo_ns(CLOCK_THREAD_CPUTIME_ID);
    // Repete a carga usada na M2 para ocupar cerca de 4 ms de CPU.
    volatile float carga = (float)pedido.id;
    do {
        carga = nav_plan_calcular_carga(carga);
    } while (tempo_ns(CLOCK_THREAD_CPUTIME_ID) - cpu_inicio < CARGA_NAV_MS * NS_POR_MS);

    resultado->ponto = nav_plan_avancar(indice_waypoint);
    resultado->waypoint = *indice_waypoint;
    resultado->cpu = tempo_ns(CLOCK_THREAD_CPUTIME_ID) - cpu_inicio;
    resultado->fim = tempo_ns(CLOCK_MONOTONIC);
}

static void *servidor(void *arg)
{
    Execucao *execucao = arg;
    unsigned int indice_waypoint = 0;
    unsigned int janela = 0;
    unsigned int concluidos = 0;
    int64_t liberacao = execucao->inicio;
    const int64_t periodo = PERIODO_MS * NS_POR_MS;
    const int64_t orcamento = ORCAMENTO_MS * NS_POR_MS;
    const int64_t reserva = RESERVA_NAV_MS * NS_POR_MS;

    struct sched_param param;
    int erro = pthread_getschedparam(pthread_self(), &execucao->politica, &param);
    if (erro != 0) {
        fprintf(stderr, "pthread_getschedparam: %s\n", strerror(erro));
        exit(EXIT_FAILURE);
    }
    execucao->prioridade = param.sched_priority;

    while (concluidos < TOTAL_PEDIDOS) {
        dormir_ate(liberacao);
        int64_t proxima = liberacao + periodo;
        int64_t cpu_inicio = tempo_ns(CLOCK_THREAD_CPUTIME_ID);

        // Só retira um pedido quando ainda cabe a reserva de atendimento.
        while (concluidos < TOTAL_PEDIDOS &&
               tempo_ns(CLOCK_THREAD_CPUTIME_ID) - cpu_inicio + reserva <= orcamento &&
               tempo_ns(CLOCK_MONOTONIC) + reserva <= proxima) {
            Pedido pedido;
            if (!retirar(execucao, &pedido))
                break;

            atender(execucao, pedido, janela, &indice_waypoint);
            ++concluidos;
        }

        // Mantém a referência absoluta, mesmo se uma janela inteira passar.
        int64_t agora = tempo_ns(CLOCK_MONOTONIC);
        if (agora > proxima) {
            int64_t janelas_passadas = (agora - proxima) / periodo;
            proxima += janelas_passadas * periodo;
            janela += (unsigned int)janelas_passadas;
        }
        liberacao = proxima;
        ++janela;
    }
    return NULL;
}

static int criar_servidor(pthread_t *thread, Execucao *execucao, int tempo_real)
{
    pthread_attr_t attr;
    int erro = pthread_attr_init(&attr);
    if (erro != 0)
        return erro;

    int politica = tempo_real ? SCHED_FIFO : SCHED_OTHER;
    struct sched_param param = {
        .sched_priority = tempo_real ? PRIORIDADE_SERVIDOR : 0
    };
    erro = pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);
    if (erro == 0)
        erro = pthread_attr_setschedpolicy(&attr, politica);
    if (erro == 0)
        erro = pthread_attr_setschedparam(&attr, &param);
    if (erro == 0)
        erro = pthread_create(thread, &attr, servidor, execucao);
    pthread_attr_destroy(&attr);
    return erro;
}

static double em_ms(int64_t tempo)
{
    return (double)tempo / NS_POR_MS;
}

static void imprimir_resultados(const Execucao *execucao)
{
    printf("NAV_PLAN | %s prioridade %d | Ts=%d ms | Cs=%d ms | prazo=%d ms\n",
           execucao->politica == SCHED_FIFO ? "SCHED_FIFO" : "SCHED_OTHER",
           execucao->prioridade, PERIODO_MS, ORCAMENTO_MS, PRAZO_NAV_MS);
    puts("Tempos em ms desde o início da agenda. Janela 0 começa em 0 ms.\n");
    puts(" ID  Janela  Chegada   Início      Fim    CPU  Resposta  Prazo    WP  (roll, pitch, yaw)");

    unsigned int no_prazo = 0;
    for (size_t i = 0; i < TOTAL_PEDIDOS; ++i) {
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
    printf("\n%u/%d no prazo, %u fora do prazo.\n",
           no_prazo, TOTAL_PEDIDOS, TOTAL_PEDIDOS - no_prazo);
}

int main(int argc, char **argv)
{
    int tempo_real = argc == 2 && strcmp(argv[1], "--rt") == 0;
    if (argc > 1 && !tempo_real) {
        fprintf(stderr, "Uso: %s [--rt]\n", argv[0]);
        return EXIT_FAILURE;
    }

    Execucao execucao = {
        .mutex = PTHREAD_MUTEX_INITIALIZER,
        .inicio = tempo_ns(CLOCK_MONOTONIC) + 100 * NS_POR_MS
    };
    pthread_t thread;
    int erro = criar_servidor(&thread, &execucao, tempo_real);
    if (erro != 0) {
        fprintf(stderr, "Falha ao criar o servidor com %s: %s\n",
                tempo_real ? "SCHED_FIFO" : "SCHED_OTHER", strerror(erro));
        return EXIT_FAILURE;
    }

    for (unsigned int i = 0; i < TOTAL_PEDIDOS; ++i) {
        dormir_ate(execucao.inicio + chegadas_ms[i] * NS_POR_MS);
        enfileirar(&execucao, i + 1);
    }

    erro = pthread_join(thread, NULL);
    if (erro != 0) {
        fprintf(stderr, "pthread_join: %s\n", strerror(erro));
        return EXIT_FAILURE;
    }
    pthread_mutex_destroy(&execucao.mutex);
    imprimir_resultados(&execucao);
    return EXIT_SUCCESS;
}
