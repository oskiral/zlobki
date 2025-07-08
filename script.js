const axios = require('axios');
const CreateCsvWriter = require('csv-writer').createObjectCsvWriter;
const cliProgress = require('cli-progress');
const fs = require('fs');
const readline = require('readline');

const URL = "https://rejestrzlobkow.mrips.gov.pl/instytucja/getListaRejestr";
const SIZE = 10;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchPage(pageNumber, type, retries = 3) {
  try {
    const res = await axios.get(URL, {
      params: { pageNumber, pageSize: SIZE, listaRejestrType: type },
      timeout: 20000
    });
    return res.data;
  } catch (err) {
    console.warn(`⚠ Błąd przy pobieraniu strony ${pageNumber}: ${err.message}`);
    if (retries > 0) {
      console.log(`...retry ${retries} pozostało`);
      await sleep(2000);
      return fetchPage(pageNumber, type, retries - 1);
    }
    throw new Error(`Nie udało się pobrać strony ${pageNumber}`);
  }
}

async function appendToCsv(filename, data, offset) {
  if (!data || data.length === 0) return;

  const fileExists = fs.existsSync(filename);

  const csvWriter = CreateCsvWriter({
    path: filename,
    header: [
      { id: 'id', title: 'ID' },
      { id: 'idApi', title: 'Identyfikator API' },
      { id: 'nazwa', title: 'Nazwa' },
      { id: 'woj', title: 'Województwo' },
      { id: 'pow', title: 'Powiat' },
      { id: 'gmina', title: 'Gmina' },
      { id: 'msc', title: 'Miejscowość' },
      { id: 'ul', title: 'Ulica' },
      { id: 'nrB', title: 'Numer Budynku' },
      { id: 'nrL', title: 'Numer Lokalu' },
      { id: 'email', title: 'Email' },
      { id: 'tel', title: 'Telefon' },
      { id: 'lDzieci', title: 'Liczba dzieci' },
      { id: 'lMiejsc', title: 'Liczba miejsc' },
      { id: 'url', title: 'Adres WWW' }
    ],
    append: fileExists
  });

  const mapped = data.map((e, idx) => ({
    id: offset + idx + 1,
    idApi: e.identyfikator,
    nazwa: e.nazwa,
    woj: e.daneAdresowe?.wojewodztwo,
    pow: e.daneAdresowe?.powiat,
    gmina: e.daneAdresowe?.gmina?.nazwa,
    msc: e.daneAdresowe?.miejscowosc?.nazwa,
    ul: e.daneAdresowe?.ulica?.nazwa,
    nrB: e.daneAdresowe?.numerBudynku,
    nrL: e.daneAdresowe?.numerLokalu,
    email: e.email,
    tel: e.telefon,
    lDzieci: e.liczbaDzieci,
    lMiejsc: e.liczbaMiejsc,
    url: e.adresWWW
  }));

  await csvWriter.writeRecords(mapped);
  console.log(`✅ Zapisano ${mapped.length} rekordów do pliku ${filename} (offset ${offset})`);
}

async function fetchAll(type, filename) {
  console.log(`📦 Pobieram dane typu ${type === 'ZK' ? 'żłobki' : 'kluby dziecięce'}...`);

  const firstPage = await fetchPage(0, type);
  if (!firstPage || typeof firstPage.totalPages !== 'number') {
    throw new Error('Niepoprawna odpowiedź API dla pierwszej strony');
  }

  const totalPages = firstPage.totalPages;
  const totalElements = firstPage.totalElements;
  const pageSize = SIZE;

  let alreadyFetched = 0;
  let startPage = 0;

  if (fs.existsSync(filename)) {
    alreadyFetched = await countCsvLines(filename) - 1; 
    startPage = Math.floor(alreadyFetched / pageSize);
    console.log(`🔄 Wykryto istniejący plik ${filename}. Już zapisano: ${alreadyFetched} rekordów.`);
    console.log(`➡️  Wznawiam pobieranie od strony ${startPage}`);
  } else {

    await appendToCsv(filename, firstPage.content, 0);
    alreadyFetched = firstPage.content.length;
    startPage = 1;
  }

  const bar = new cliProgress.SingleBar({
    stream: process.stderr,
    clearOnComplete: true,
    hideCursor: true
  }, cliProgress.Presets.shades_classic);

  bar.start(totalPages, startPage);

  let buffer = [];
  let globalCount = alreadyFetched;

  for (let i = startPage; i < totalPages; i++) {
    let page;
    let retries = 3;
    while (retries > 0) {
      try {
        page = await fetchPage(i, type);
        break;
      } catch (err) {
        retries--;
        console.warn(`⚠️  Błąd przy pobieraniu strony ${i}, retry=${retries}`);
        if (retries === 0) throw err;
        await sleep(2000);
      }
    }

    if (page.content?.length) {
      buffer.push(...page.content);
      globalCount += page.content.length;
    }

    bar.update(i + 1);

    if (i % 10 === 0 || i === totalPages - 1) {
      await appendToCsv(filename, buffer, globalCount - buffer.length);
      buffer = [];
    }

    await sleep(2000);
  }

  bar.stop();
  console.log(`✅ Pobrano łącznie ${globalCount} rekordów (${globalCount - alreadyFetched} nowych).`);
  return globalCount;
}

function countCsvLines(filename) {
  return new Promise((resolve, reject) => {
    let lines = 0;
    const reader = readline.createInterface({
      input: fs.createReadStream(filename)
    });
    reader.on('line', () => lines++);
    reader.on('close', () => resolve(lines));
    reader.on('error', reject);
  });
}

async function main() {
  const typeArg = process.argv[2] || 'ZK';
  const valid = ['ZK', 'KL', 'ALL'];
  if (!valid.includes(typeArg)) {
    console.error(`Błąd: niepoprawny typ (dostępne: ZK, KL, ALL)`);
    process.exit(1);
  }

  try {
    if (typeArg === 'ALL') {
      await fetchAll('ZK', 'zlobki.csv');
      await fetchAll('KL', 'kluby.csv');
    } else {
      const filename = typeArg === 'ZK' ? 'zlobki.csv' : 'kluby.csv';
      await fetchAll(typeArg, filename);
    }
    console.log('✅ Wszystko ukończone pomyślnie!');
  } catch (err) {
    console.error('❌ Wystąpił błąd:', err.message);
    process.exit(1);
  }
}

main();
