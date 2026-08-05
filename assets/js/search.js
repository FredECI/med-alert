(function () {
  "use strict";

  // A tabela cresce para sempre (uma linha por vaga já descoberta). Todas as
  // linhas continuam no HTML — busca e filtro precisam enxergar o histórico
  // inteiro — mas só as mais recentes aparecem de saída, para a página não
  // abrir com centenas de itens.
  var INITIAL_VISIBLE = 50;

  var table = document.getElementById("jobs-table");
  if (!table) return;
  var tbody = table.querySelector("tbody");
  if (!tbody) return;

  var input = document.getElementById("job-search");
  var filtersBox = document.getElementById("filters");
  var clearButton = document.getElementById("clear-filters");
  var summary = document.getElementById("filters-summary");
  var chips = Array.prototype.slice.call(document.querySelectorAll(".chip[data-filter]"));
  var rows = Array.prototype.slice.call(tbody.querySelectorAll("tr"));

  var state = { region: [], type: [], text: "" };
  var limitActive = rows.length > INITIAL_VISIBLE;

  // --- Lógica de correspondência -------------------------------------------
  // Dentro de uma dimensão vale OU (marcar duas regiões mostra as duas);
  // entre dimensões vale E (região E tipo E texto).
  function matches(row) {
    if (state.region.length && state.region.indexOf(row.getAttribute("data-region")) === -1) return false;
    if (state.type.length && state.type.indexOf(row.getAttribute("data-type")) === -1) return false;
    if (state.text && row.textContent.toLowerCase().indexOf(state.text) === -1) return false;
    return true;
  }

  function isFiltering() {
    return state.region.length > 0 || state.type.length > 0 || state.text !== "";
  }

  // --- Estado na URL --------------------------------------------------------
  // Permite mandar para alguém um link já filtrado.
  function readUrl() {
    var params = new URLSearchParams(window.location.search);
    state.region = (params.get("regiao") || "").split(",").filter(Boolean);
    state.type = (params.get("tipo") || "").split(",").filter(Boolean);
    state.text = (params.get("busca") || "").trim().toLowerCase();
    if (input && state.text) input.value = params.get("busca");
  }

  function writeUrl() {
    var params = new URLSearchParams();
    if (state.region.length) params.set("regiao", state.region.join(","));
    if (state.type.length) params.set("tipo", state.type.join(","));
    if (state.text) params.set("busca", state.text);
    var query = params.toString();
    // replaceState e não pushState: filtrar não deveria encher o botão de
    // voltar do navegador com um passo por clique.
    window.history.replaceState(null, "", query ? "?" + query : window.location.pathname);
  }

  // --- Renderização ---------------------------------------------------------
  function syncChips() {
    chips.forEach(function (chip) {
      var lista = state[chip.getAttribute("data-filter")] || [];
      var ativo = lista.indexOf(chip.getAttribute("data-value")) !== -1;
      chip.setAttribute("aria-pressed", ativo ? "true" : "false");
    });
    if (clearButton) clearButton.hidden = !isFiltering();
  }

  function render() {
    var visiveis = 0;
    var encontrados = 0;

    rows.forEach(function (row) {
      if (!matches(row)) {
        row.style.display = "none";
        return;
      }
      encontrados += 1;
      // Durante um filtro o teto é ignorado: esconder resultado que a pessoa
      // pediu explicitamente seria pior do que uma lista longa.
      var cabe = !limitActive || isFiltering() || visiveis < INITIAL_VISIBLE;
      row.style.display = cabe ? "" : "none";
      if (cabe) visiveis += 1;
    });

    if (summary) {
      summary.textContent = isFiltering()
        ? encontrados + " de " + rows.length + " vagas correspondem ao filtro."
        : "";
    }

    var showAll = document.getElementById("show-all-jobs");
    if (showAll) {
      var ocultas = limitActive && !isFiltering() ? Math.max(0, encontrados - visiveis) : 0;
      showAll.hidden = ocultas === 0;
      showAll.textContent = "Mostrar todas as " + encontrados + " vagas (+" + ocultas + ")";
    }

    syncChips();
  }

  // --- Ligações -------------------------------------------------------------
  chips.forEach(function (chip) {
    chip.addEventListener("click", function () {
      var dimensao = chip.getAttribute("data-filter");
      var valor = chip.getAttribute("data-value");
      var lista = state[dimensao];
      var posicao = lista.indexOf(valor);
      if (posicao === -1) lista.push(valor);
      else lista.splice(posicao, 1);
      writeUrl();
      render();
    });
  });

  if (input) {
    input.addEventListener("input", function () {
      state.text = input.value.trim().toLowerCase();
      writeUrl();
      render();
    });
  }

  if (clearButton) {
    clearButton.addEventListener("click", function () {
      state.region = [];
      state.type = [];
      state.text = "";
      if (input) input.value = "";
      writeUrl();
      render();
    });
  }

  var showAll = document.getElementById("show-all-jobs");
  if (showAll) {
    showAll.addEventListener("click", function () {
      limitActive = false;
      render();
    });
  }

  // Os filtros só aparecem agora: eles dependem inteiramente de JavaScript, e
  // um botão que não faz nada é pior do que botão nenhum. Sem JS a página
  // continua útil, com a tabela completa.
  if (filtersBox) filtersBox.hidden = false;

  readUrl();
  render();
})();
