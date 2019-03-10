const readline = require('readline')
const csvParse = require('csv-parse')
const moment = require('moment')
const fs = require('fs')

const HEADER_VALS = {
	AutoritateContractanta: 'contractingAuthority',
	Castigator: 'supplier',
	Descriere: 'directAcquisitionName',
	NumarContract: 'uniqueIdentificationCode',
	DataContract: 'finalizationDate',
	ValoareRON: 'estimatedValueRon',
	ValoareEUR: 'estimatedValueOtherCurrency',
	CPVCode: 'cpvCode',
}

function getFilenames(inputFolder) {
	return fs.readdirSync(inputFolder)
}

function getMonday(newD) {
	const d = new Date(newD);
	const day = d.getDay();
	const diff = (d.getDate() - day) + (day === 0 ? -6 : 1); // adjust when day is sunday

	d.setDate(diff)
	d.setHours(0 - (d.getTimezoneOffset() / 60));
	d.setMinutes(0);
	d.setSeconds(0);
	d.setMilliseconds(0)

	return new Date(d);
}

async function importFile(filePath) {
	const start = Date.now()
	const Contracts = await require('./contracts')()

	const input = fs.createReadStream(filePath)

	const rl = readline.createInterface({
		input,
		terminal: false,
	})

	const parser = csvParse({
		delimiter: '^',
		skip_lines_with_error: true,
		columns: headers => headers.map(header => HEADER_VALS[header] || header),
		// columns: true,
		cast(value, context) {
			if (context.column !== 'finalizationDate') return value

			try {
				return moment(value.replace(' ', 'T')).toDate()
			} catch (ex) {
				console.error('\n\nex', ex)
				console.log('value ', value, '\n\n')
				return new Date()
			}
		},
	})

	parser.on('readable', () => {
		let record = parser.read();
		while (record) {
			try {
				const contract = Object.keys(HEADER_VALS).reduce((prev, header) => { // eslint-disable-line no-loop-func
					const colName = HEADER_VALS[header];
					const newPrev = { ...prev }
					let value = record[colName]

					if (colName === 'contractingAuthority') {
						value = `${record.AutoritateContractantaCUI} ${value}`
					} else if (colName === 'supplier') {
						value = `${record.CastigatorCUI} ${record[Object.keys(record)[0]]}` // something wrong with Castigator column :/
					} else if (colName.includes('estimatedValue')) {
						value = parseFloat(value)
					}
					newPrev[colName] = value
					return newPrev
				}, { csvImport: true })

				// console.log('got contract', Contracts, contract)
				contract.week = getMonday(contract.finalizationDate)
				Contracts.update({ uniqueIdentificationCode: contract.uniqueIdentificationCode }, { $set: contract }, { upsert: true }, (err) => { if (err) console.error('ERROR', err) })
			} catch (ex) {
				console.error('error when parsing record', record, ex)
			}
			record = parser.read()
		}
	})

	parser.on('error', err => console.error('parser got error', err))

	rl.on('line', line => {
		parser.write(`${line}\r\n`)
	})

	return new Promise(resolve => {
		rl.on('close', function () {
			input.close()
			parser.end()
			resolve()
			console.log(`Finish uploading, time taken: ${Date.now() - start}`);
		});
	})
}

async function main() {
	const fileNames = getFilenames('./in')

	for (const filePath of fileNames) {
		console.log('importing file ', filePath)
		await importFile(`./in/${filePath}`)
	}
}

main()
