(function () {
  "use strict";

  // A tabela cresce para sempre (uma linha por vaga já descoberta). Todas as
  // linhas continuam no HTML — a busca precisa enxergar o histórico inteiro —
  // mas só as mais recentes aparecem de saída, para a página não abrir com
  // centenas de itens de uma vez.
  var INITIAL_VISIBLE = 50;

  var input = document.getElementById("job-search");
  var table = document.getElementById("jobs-table");
  var showAllButton = document.getElementById("show-all-jobs");
  if (!table) {
    return;
  }

  var tbody = table.querySelector("tbody");
  if (!tbody) {
    return;
  }

  var rows = Array.prototype.slice.call(tbody.querySelectorAll("tr"));
  var limitActive = rows.length > INITIAL_VISIBLE;

  function render() {
    var query = input ? input.value.trim().toLowerCase() : "";
    var matched = 0;

    rows.forEach(function (row) {
      var matches = query === "" || row.textContent.toLowerCase().indexOf(query) !== -1;
      if (!matches) {
        row.style.display = "none";
        return;
      }

      matched += 1;
      // Durante uma busca o limite é ignorado: esconder resultado que o
      // usuário pediu explicitamente seria pior do que uma lista longa.
      var withinLimit = !limitActive || query !== "" || matched <= INITIAL_VISIBLE;
      row.style.display = withinLimit ? "" : "none";
    });

    if (showAllButton) {
      var hidden = limitActive && query === "" ? Math.max(0, matched - INITIAL_VISIBLE) : 0;
      showAllButton.hidden = hidden === 0;
      showAllButton.textContent = "Mostrar todas as " + matched + " vagas (+" + hidden + ")";
    }
  }

  if (input) {
    input.addEventListener("input", render);
  }

  if (showAllButton) {
    showAllButton.addEventListener("click", function () {
      limitActive = false;
      render();
    });
  }

  render();
})();
