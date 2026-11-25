#include "nav_plan.h"

// Pontos da rota da M2 com roll, pitch e yaw em graus.
static const Waypoint rota[] = {
    {0.0f, 0.0f, 0.0f},
    {5.0f, 0.0f, 30.0f},
    {0.0f, 5.0f, 60.0f},
};

Waypoint nav_plan_avancar(unsigned int *indice)
{
    *indice = (*indice + 1U) % (sizeof(rota) / sizeof(rota[0]));
    return rota[*indice];
}

float nav_plan_calcular_carga(float semente)
{
    // Reaproveita a carga da M2. O volatile preserva os cálculos com otimização.
    volatile float valor = semente;

    for (unsigned int i = 0; i < 24000U; ++i) {
        const float entrada = (float)(i & 0x0FU) * 0.0001f;
        valor = (valor * 0.9997f) + entrada;
        valor = (valor * 0.9999f) - (entrada * 0.0002f);
    }

    return valor;
}
