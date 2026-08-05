// ATENÇÃO: estas chaves precisam ser IDÊNTICAS às de medalert/taxonomy.py.
// Elas são o contrato entre os dois lados: o Worker grava as preferências com
// estas strings e o robô (Python) filtra as vagas comparando com as colunas
// `region` e `job_type` do banco. Se divergirem, o filtro para de casar e o
// assinante simplesmente deixa de receber — em silêncio, sem erro nenhum.

export const REGIONS = [
  { key: "capital_metropolitana", label: "Capital e Região Metropolitana" },
  { key: "regiao_dos_lagos", label: "Região dos Lagos" },
  { key: "norte_fluminense", label: "Norte Fluminense (Macaé e região)" },
  { key: "outras_rj", label: "Outras regiões do RJ" },
  { key: "estadual_nacional", label: "Estadual / Nacional" },
];

export const JOB_TYPES = [
  { key: "concurso", label: "Concurso público" },
  { key: "processo_seletivo", label: "Processo seletivo / temporário" },
  { key: "residencia", label: "Residência médica" },
  { key: "emprego", label: "Emprego CLT / privado" },
  { key: "noticia", label: "Notícia / radar" },
];

export const REGION_KEYS = REGIONS.map((r) => r.key);
export const JOB_TYPE_KEYS = JOB_TYPES.map((t) => t.key);

// Padrão de quem acabou de chegar: todas as regiões e todos os tipos MENOS
// "notícia/radar", que é o mais ruidoso (matéria sobre concurso, não a vaga
// em si). O assistente mostra isso pré-marcado para o usuário ajustar.
export const DEFAULT_REGIONS = [...REGION_KEYS];
export const DEFAULT_JOB_TYPES = JOB_TYPE_KEYS.filter((k) => k !== "noticia");

export function labelFor(list, key) {
  const found = list.find((item) => item.key === key);
  return found ? found.label : key;
}
