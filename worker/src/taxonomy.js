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

// Famílias, não especialidades individuais: com as 55 do CFM, "Cardiologia"
// teria meia dúzia de vagas no acervo inteiro e o filtro pareceria quebrado.
export const SPECIALTIES = [
  { key: "clinica_medica", label: "Clínica médica" },
  { key: "cirurgia", label: "Cirurgia e especialidades cirúrgicas" },
  { key: "pediatria", label: "Pediatria e neonatologia" },
  { key: "gineco_obstetricia", label: "Ginecologia e obstetrícia" },
  { key: "anestesiologia", label: "Anestesiologia" },
  { key: "intensiva_emergencia", label: "Terapia intensiva e emergência" },
  { key: "atencao_primaria", label: "Saúde da família e atenção básica" },
  { key: "psiquiatria", label: "Psiquiatria e saúde mental" },
  { key: "diagnostico", label: "Diagnóstico por imagem e patologia" },
  { key: "especialidades_clinicas", label: "Outras especialidades clínicas" },
  { key: "trabalho_pericia", label: "Medicina do trabalho, perícia e auditoria" },
  // Opção de verdade, não ausência de resposta: a maioria das vagas não diz a
  // especialidade em lugar nenhum. Desmarcar isto some com quase tudo, então
  // vem marcado e o assistente avisa o que significa.
  { key: "nao_especificada", label: "Sem especialidade identificada" },
];

export const REGION_KEYS = REGIONS.map((r) => r.key);
export const JOB_TYPE_KEYS = JOB_TYPES.map((t) => t.key);
export const SPECIALTY_KEYS = SPECIALTIES.map((e) => e.key);

// Padrão de quem acabou de chegar: todas as regiões e todos os tipos MENOS
// "notícia/radar", que é o mais ruidoso (matéria sobre concurso, não a vaga
// em si). O assistente mostra isso pré-marcado para o usuário ajustar.
export const DEFAULT_REGIONS = [...REGION_KEYS];
export const DEFAULT_JOB_TYPES = JOB_TYPE_KEYS.filter((k) => k !== "noticia");
// Especialidades vêm TODAS marcadas, inclusive "sem especialidade
// identificada". Quem não quiser mexer continua recebendo tudo, que é o
// comportamento que a pessoa já tinha antes de este filtro existir.
export const DEFAULT_SPECIALTIES = [...SPECIALTY_KEYS];

export function labelFor(list, key) {
  const found = list.find((item) => item.key === key);
  return found ? found.label : key;
}
