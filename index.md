---
layout: default
title: MedAlert RJ
---

{% include header.html %}

<table id="jobs-table">
  <thead>
    <tr>
      <th>Data de Descoberta</th>
      <th>Título do Processo Seletivo</th>
      <th>Link Oficial</th>
    </tr>
  </thead>
  <tbody>
    {% for job in site.data.jobs %}{% include job_row.html job=job %}{% endfor %}
  </tbody>
</table>

<script src="{{ '/assets/js/search.js' | relative_url }}"></script>
