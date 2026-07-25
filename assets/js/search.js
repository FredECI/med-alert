(function () {
  "use strict";

  var input = document.getElementById("job-search");
  var table = document.getElementById("jobs-table");
  if (!input || !table) {
    return;
  }

  var tbody = table.querySelector("tbody");
  if (!tbody) {
    return;
  }

  var rows = Array.prototype.slice.call(tbody.querySelectorAll("tr"));

  input.addEventListener("input", function () {
    var query = input.value.trim().toLowerCase();
    rows.forEach(function (row) {
      var matches = query === "" || row.textContent.toLowerCase().indexOf(query) !== -1;
      row.style.display = matches ? "" : "none";
    });
  });
})();
