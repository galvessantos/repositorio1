-- Vehicle cache table for caching vehicle data from external API
CREATE TABLE IF NOT EXISTS vehicle_cache (
    id BIGSERIAL PRIMARY KEY,
    external_id BIGINT,
    credor VARCHAR(255),
    data_pedido DATE,
    contrato VARCHAR(100) UNIQUE,
    placa VARCHAR(15),
    modelo VARCHAR(100),
    uf VARCHAR(2),
    cidade VARCHAR(100),
    cpf_devedor VARCHAR(14),
    protocolo VARCHAR(100) UNIQUE,
    etapa_atual VARCHAR(100),
    status_apreensao VARCHAR(100),
    ultima_movimentacao DATE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    api_sync_date TIMESTAMP NOT NULL
);

-- Indexes for better performance
CREATE INDEX IF NOT EXISTS idx_vehicle_cache_sync_date ON vehicle_cache(api_sync_date);
CREATE INDEX IF NOT EXISTS idx_vehicle_cache_filters ON vehicle_cache(data_pedido, credor, uf, cidade);
CREATE INDEX IF NOT EXISTS idx_vehicle_cache_placa ON vehicle_cache(placa);
CREATE INDEX IF NOT EXISTS idx_vehicle_cache_cpf ON vehicle_cache(cpf_devedor);