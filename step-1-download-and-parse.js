const fs = require("fs")

const { max } = require("d3")
const http = require("axios")
const { default: Queue } = require("p-queue")

const stations = require("./stations.json")

async function fetchDlyFile(stationId) {
  const url = `https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/all/${stationId}.dly`
  const res = await http(url)

  if (res.status !== 200) throw new Error(res.statusText)

  return res.data
}

const dlyLayout = [
  ["id", 0, 11],
  ["year", 11, 15],
  ["month", 15, 17],
  ["element", 17, 21],
  ["day1", 21, 26],
  ["day2", 29, 34],
  ["day3", 37, 42],
  ["day4", 45, 50],
  ["day5", 53, 58],
  ["day6", 61, 66],
  ["day7", 69, 74],
  ["day8", 77, 82],
  ["day9", 85, 90],
  ["day10", 93, 98],
  ["day11", 101, 106],
  ["day12", 109, 114],
  ["day13", 117, 122],
  ["day14", 125, 130],
  ["day15", 133, 138],
  ["day16", 141, 146],
  ["day17", 149, 154],
  ["day18", 157, 162],
  ["day19", 165, 170],
  ["day20", 173, 178],
  ["day21", 181, 186],
  ["day22", 189, 194],
  ["day23", 197, 202],
  ["day24", 205, 210],
  ["day25", 213, 218],
  ["day26", 221, 226],
  ["day27", 229, 234],
  ["day28", 237, 242],
  ["day29", 245, 250],
  ["day30", 253, 258],
  ["day31", 261, 266],
]

function parseDlyFile(text) {
  const lines = text.split("\n")
  return lines.map((line) => {
    const parsedLine = dlyLayout.reduce((accum, next) => {
      const key = next[0]
      const value = line.slice(next[1], next[2]).trim()
      accum[key] = key.includes("day") ? parseFloat(value) : value
      return accum
    }, {})

    const {
      id,
      year,
      month,
      element,
      day1,
      day2,
      day3,
      day4,
      day5,
      day6,
      day7,
      day8,
      day9,
      day10,
      day11,
      day12,
      day13,
      day14,
      day15,
      day16,
      day17,
      day18,
      day19,
      day20,
      day21,
      day22,
      day23,
      day24,
      day25,
      day26,
      day27,
      day28,
      day29,
      day30,
      day31,
    } = parsedLine

    return {
      id,
      year,
      month,
      element,
      values: [
        day1,
        day2,
        day3,
        day4,
        day5,
        day6,
        day7,
        day8,
        day9,
        day10,
        day11,
        day12,
        day13,
        day14,
        day15,
        day16,
        day17,
        day18,
        day19,
        day20,
        day21,
        day22,
        day23,
        day24,
        day25,
        day26,
        day27,
        day28,
        day29,
        day30,
        day31,
      ],
    }
  })
}

async function saveData(station, data) {
  const { id, lat, long } = station
  const toSave = {
    id,
    lat,
    long,
    data,
  }
  await fs.promises.writeFile(
    `./data/${station.id}.json`,
    JSON.stringify(toSave, null, 2)
  )
}

const queue = new Queue({ concurrency: 8 })
let activeStations = 0

stations.forEach(async (station) => {
  queue.add(async () => {
    console.log(`Processing ${station.id}`)
    const data = await fetchDlyFile(station.id)
    const parsed = parseDlyFile(data)
    const tmax = parsed.filter(d => d.element === 'TMAX')
    const maxYear = max(tmax, (d) => parseInt(d.year))
    const isActiveStation = maxYear === 2020
    console.log(station.id, maxYear, isActiveStation)

    if (!isActiveStation) return
    activeStations += 1
    await saveData(station, parsed)
  })
})

queue.onIdle().then(() => {
  console.log(
    `All done, found ${activeStations} active stations out of ${stations.length} stations`
  )
})
