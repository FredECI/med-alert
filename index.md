---
layout: default
title: MedAlert RJ
---

{% include styles.html %}
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

<p><button type="button" id="show-all-jobs" hidden></button></p>

{%- comment -%}
  O `?v=` muda a cada build e existe para furar o cache do navegador.
  HTML e JavaScript são guardados em entradas de cache separadas: sem isso,
  um visitante que já esteve aqui recebe o HTML novo com o JS velho — e o
  sintoma é silencioso, porque a página carrega, só que os filtros não
  funcionam. Foi exatamente o que aconteceu quando os chips estrearam.
{%- endcomment -%}
<script src="{{ '/assets/js/search.js' | relative_url }}?v={{ site.time | date: '%s' }}"></script>
