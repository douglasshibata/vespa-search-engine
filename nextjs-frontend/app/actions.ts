'use server';

export async function searchVespa(queryTerm: string) {
  const vespaQueryUrl = `http://localhost:8080/search/?yql=select * from sources * where default contains "${queryTerm}";`;

  const response = await fetch(vespaQueryUrl, {
    method: 'GET',
    cache: 'no-store',
  });

  if (!response.ok) {
    throw new Error('Failed to fetch from Vespa');
  }

  const data = await response.json();
  return data.root?.children || [];
}
