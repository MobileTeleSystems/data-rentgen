from data_rentgen.consumer.openlineage.job_facets.base import OpenLineageJobFacet


class OpenLineageSqlJobFacet(OpenLineageJobFacet):
    query: str
