from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="COPILOT_", env_file=".env")

    # LLM
    model_path: str = "/models/llama-3.2-3b-instruct.Q4_K_M.gguf"
    n_ctx:      int = 4096
    n_gpu_layers: int = 0  # 0 = CPU; set > 0 for GPU offload
    temperature:  float = 0.1
    max_tokens:   int = 512

    # Services
    gateway_url:    str = "http://api-gateway:8080"
    gateway_token:  str = ""
    port:           int = 8081

    # Storage
    feedback_db_path: str = "/data/feedback.db"
