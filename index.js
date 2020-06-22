'use strict'

const fs = require('fs');
const os = require('os');
const path = require('path');
let offset = 0;
let errorfile = "error.csv";
let outfile = "output.csv";

function bytes_to_int (bytes, length) {
    bytes = new Int32Array(bytes.slice(0, length));
    return bytes.reduce((accumulator, currentValue) => accumulator * 256 + Number(currentValue), 0);
}

function int_to_bytes (value, length) {
    let result = []

    for (let i = 0; i < length; ++i) {
        result.push(value >> (i * 8) & 0xff);
    }

    result = result.reverse();
    return result;
}

async function time (fd) {
    const context = {
        'year': 0, // default values
        'month': 0,
        'day': 0,
        'hour': 0,
        'minute': 0,
        'second': 0
    };
    const keys = Object.keys(context);
    let isInvalid = false;

    for (let key of keys) {
        let buffer = Buffer.alloc(100);
        const num = await fs.readSync(fd, buffer, 0, 1, offset);
        offset += 1;
        context[key] = buffer.readUIntBE(0, num);
        buffer = null; // free memory
    }
    const timeStr = `20${context.year}/${context.month}/${context.day} ${context.hour}:${context.minute}:${context.second}`;
   
    if (context.year > new Date().getFullYear()) {
        isInvalid = true;
    }
    if (context.month > 12) {
        isInvalid = true;
    }
    if (context.hour > 23) {
        isInvalid = true;
    }
    if (context.second > 60) {
        isInvalid = true;
    }
    if (context.minute > 60) {
        isInvalid = true;
    }

    return [ timeStr, isInvalid ];
}

async function duration (fd) {
    const readuint = (os.endianness() === 'LE') ? 'readUIntLE' : 'readUIntBE';
    let buffer = Buffer.alloc(4);
    const num = await fs.readSync(fd, buffer, 0, 4, offset);

    if (!num) {
        return null;
    }
    offset += 4;
    const value = buffer[readuint](0, num);
    return '' + value;
}

async function address_nature_indicator (fd) {
    let buffer = Buffer.alloc(1);
    const num = await fs.readSync(fd, buffer, 0, 1, offset);
    offset += 1;
    const indicator = bytes_to_int(buffer, num);
    return '' + indicator;
}

async function number (fd, limit) {
    let count = 0;
    let digits = [];
    let byte = 0x00;

    while (count < limit) {
        count += 1;
        let buffer = Buffer.alloc(1);
        const num = await fs.readSync(fd, buffer, 0, 1, offset);
        offset += 1;

        const digit_in_nibble1 = (bytes_to_int(buffer, num) >> 4) & 0x0F;

        if (digit_in_nibble1 === 0x0F) {
            continue;
        }

        digits.push(digit_in_nibble1);

        const digit_in_nibble2 = bytes_to_int(buffer, num) & 0x0F;

        if (digit_in_nibble2 === 0x0F) {
            continue;
        }

        digits.push(digit_in_nibble2);
    }
    let result = '';
    digits.map((digit) => result += `${digit}`);
    
    return result;
}

async function move_forward (fd, length) {
    const buffer = Buffer.alloc(length);
    await fs.readSync(fd, buffer, 0, length, offset);
    offset += length;
}

async function fixed_ordinary (fd, csn, netTypeStr, billTypeStr) {
    await move_forward(fd, 3) // 3 bytes moved ahead to come to byte 9:
   
    let [answerTime, isAnswerTimeInvalid] = await time(fd) //ans_time:: offset 9, 6 bytes, yymmddhhmmss

    if (isAnswerTimeInvalid) {
        console.log('Invalid answerTime #try going back and read again: ', answerTime);
        offset -= 7;
        let [conversionEndtime, isCoversationTimeInvalid] = await time(fd)
        console.log('Corrected answerTime ---------------------------  ', answerTime);
    }

    let [conversionEndtime, isCoversationTimeInvalid] = await time(fd) // conversation_end_Time #offset 15, length 6 bytes yymmddhhmmss
    if (isCoversationTimeInvalid) {
        console.log('Invalid CoversationTime #try going back and read again: '  + conversionEndtime);
        offset -= 7;
        let [conversionEndtime, isCoversationTimeInvalid] = await time(fd)
        console.log(' Corrected conversationEndTime -------------------------' + conversionEndtime);
    }

    const callDuration = await duration(fd); //conversation_Time #offset 21, length 4 bytes - long integer
    await move_forward(fd, 2) //2 bytes moved ahead to come to byte 27:
    await address_nature_indicator(fd); //caller_number_address_nature_indicator #offset 27, length 1
    const callerNumber = await number(fd, 10); //caller_number #offset 28, length =10, BCD code,
    await move_forward(fd, 2) //2 bytes moved ahead to come to byte 40:
    await address_nature_indicator(fd); //called_address_naure_indicator #offset 40, 1
    const calledNumber = await number(fd, 10); //called_number #offset 41, 10
    await move_forward(fd, 44) //44 bytes moved ahead to come to byte 95:
    await address_nature_indicator(fd); //connected_number_address_nature_indicator #offset 95, 1
    const contactedNumber = await number(fd, 10); //connected_number #offset 96, 10, bcd coded
    await move_forward(fd, 70) //70 bytes moved ahead to come to byte 176:
    const dialedNumber = await number(fd, 12); //dialed_number #offset 176, 12 bcd coded
    await move_forward(fd, 62) //# takes to start of next CDR..
    
    const returnStr = answerTime + ";" + conversionEndtime + ";" + callDuration + ";" + callerNumber + ";" + calledNumber + ";" + contactedNumber; //+ ";" + dialedNumber
    const data_cdr = [`${csn}`, netTypeStr, billTypeStr, answerTime, conversionEndtime, callDuration, callerNumber, calledNumber, contactedNumber]; //, dialedNumber)
    // pushtotable(add_cdr, data_cdr)   

    return [returnStr, isAnswerTimeInvalid, isCoversationTimeInvalid];
}

async function fixed_in (fd, csn, netTypeStr, billTypeStr) {
    // record_type #offset 12, length =1  -- 0x03 means IN
    await move_forward(fd, 17); //6 bytes were already read, moved ahead to come to byte 23:	
    
    /** #caller_number_address_indicator #offset 19, length=1, 
	
    #caller_number_description #offset 20, length-14 
    #7 bits: Address value indicator
    #1 bit : odd/even indicator
    #2 bits: mask_indicator
    #2 bits: address+presentation restriction indicator
    #3 bits: number plan indicator
    #1 bit: called bit number indicator
    #5 bit: number length
    #3 bit: spared
    #caller Number Offset: 20+ 3 = 23, lentgh 11 bytes: content bcd code...*/
    const callerNumber = await number(fd, 11);    //caller_number #offset 28, length =10, BCD code,

    await move_forward(fd, 19); // reach to 53 bytes	
    /**
    #called_number_description #offset 50, length 14 bytes
    #7 bits: Address value indicator
    #1 bit : odd/even indicator
    #2 bits: spared
    #2 bits: spared
    #3 bits: number plan indicator
    #1 bit: called bit number indicator
    #5 bit: number length
    #3 bit: spared*/
    const calledNumber = await number(fd, 11);   //called number offset = 50 + 3, lentgh = 11 bytes: content bcd code...

    await move_forward(fd, 4); // reach to 68 bytes 
    /**       
    #destination_number_description #offset 65, length 14 bytes
    #7 bits: Address value indicator
    #1 bit : odd/even indicator
    #2 bits: spared
    #2 bits: spared
    #3 bits: number plan indicator
    #1 bit: called bit number indicator
    #5 bit: number length
    #3 bit: spared */
    const contactedNumber = await number(fd, 11) // destinationNumber offset = 65+ 3, lentgh = 11 bytes: content bcd code...
    
    //designated_charging_number #offset 81, length 11 byte BCD code

    await move_forward(fd, 17); // reach to 96 bytes        
    const [answerTime, isAnswerTimeInvalid] = await time(fd); //offset 96, 6 bytes, yymmddhhmmss
    
    
    const [conversionEndtime, isCoversationTimeInvalid] = await time(fd); // offset 102, length 6 bytes

    const callDuration = await duration(fd); //offset 108, length 4 bytes - hhh, mm, ss, t
    await move_forward(fd, 138); //takes to start of next CDR.. 250-112 = 138
    const returnStr = answerTime + ";" + conversionEndtime + ";" + callDuration + ";" + callerNumber + ";" + calledNumber + ";" + contactedNumber; //+ ";" + dialedNumber	
    const data_cdr = [`${csn}`, netTypeStr, billTypeStr, answerTime, conversionEndtime, callDuration, callerNumber, calledNumber, contactedNumber]; //, dialedNumber)
 
    //pushtotable(add_cdr, data_cdr)    
    return [returnStr, isAnswerTimeInvalid, isCoversationTimeInvalid]
}

async function processFile (inputFile) {
    const fd = await fs.openSync(inputFile, 'r');
    let num;
    let netTypeStr;
    let csn;
    let net_type;
    let billTypeStr;
    let bill_type;
    let returnStr;
    let isAnswerTimeInvalid;
    let isCoversationTimeInvalid;

    console.log(`Processing ${inputFile}`);

    while (1) {
        csn = await duration (fd);
        if (csn === null) {
            break;
        }

        let buffer = Buffer.alloc(1);
        num = await fs.readSync(fd, buffer, 0, 1, offset);
        offset += 1;
        net_type = bytes_to_int(buffer, num);

        if (net_type === 11) {
            netTypeStr = 'Fixed Network Bill';
        } else if (net_type === 22) {
            netTypeStr = 'Mobile Network Bill';
        } else {
            netTypeStr = 'Error';
        }

        //Attempt to correct the CDR::

        if (netTypeStr === 'Error') {
            //..go back few bytes... 4+1 + error bytes 7.... 12 bytes... keep reading byte.. till it is non-zero.. it should be start of csn.. go from here..		
            console.log("Invalid NetType #try going back and read again: "  + netTypeStr);

            offset -= 12;

            let count = 0;
            while (1) {
                let buffer = Buffer.alloc(1);

                let num = await fs.readSync(fd, buffer, 0, 1, offset);
                offset += 1;
                console.log(buffer);			
                count += 1;
                if (count >= 12) {
                    console.log(count)
                    break;
                } else {
                    if (bytes_to_int(buffer, num) === 0) {
                        console.log("contiue reading next byte");			
                        continue;
                    } else{
                        console.log("#this seems to be first byte for CSN");
                        offset -= 1;
                        // attempt read csn & network type again
                        csn = await duration(fd);
                        if (csn === null) {
                            break;
                        }

                        // print("New csn" )
                        let buffer = Buffer.alloc(1);
                        let num = await fs.readSync(fd, buffer, 0, 1, offset);
                        offset += 1;

                        net_type = bytes_to_int(buffer, num);
                        if (net_type === 11) {
                            netTypeStr = 'Fixed Network Bill';
                        }else if (net_type === 22) {
                            netTypeStr = 'Mobile Network Bill';
                        }
                        else {
                            netTypeStr = 'Error';
                        }
                        console.log(" Corrected NetType -------------------------" + netTypeStr) ;
                        break;	
                    }
                }
            }	
        }

        buffer = Buffer.alloc(1);
        num = await fs.readSync(fd, buffer, 0, 1, offset);
        offset += 1;
        bill_type = bytes_to_int(buffer, num);
        billTypeStr = '';
        
        if (bill_type === 1) {
            billTypeStr = 'Detailed Ticket';
        } else if (bill_type === 2) {
            billTypeStr='DBO Call Record';
        } else if (bill_type === 3) {
            billTypeStr='In Record';
        } else if (bill_type === 5) {
            billTypeStr='TAX Record';
        } else if (bill_type === 0xF0) {
            billTypeStr='Meter Table Ticket';
        } else if (bill_type === 0xF1) {
             billTypeStr='Meter Table Statistics';
        } else if (bill_type === 0xF2) {
            billTypeStr='Trunk Duration Statistics';
        } else if (bill_type === 0xF3) {
            billTypeStr='Free Call Statistics';
        } else if (bill_type === 0xF4) {
            billTypeStr='SCCP Meter Table Ticket';
        } else if (bill_type === 0xFF) {
            billTypeStr='Warn Ticket';
        } else if (bill_type === 0x55) {
            billTypeStr='Failed Call Ticekt';
        } else {
            billTypeStr ='Error';
        }
        
        // console.log(billTypeStr, net_type, bill_type, inputfile);

        returnStr = "";
        if ((net_type === 11) & (bill_type === 3)) {
            [returnStr, isAnswerTimeInvalid, isCoversationTimeInvalid] = await fixed_in(fd, csn, netTypeStr, billTypeStr);
            if (isAnswerTimeInvalid | isCoversationTimeInvalid) {
                const payload = `${csn}` + ";" + netTypeStr + ";" + billTypeStr + ";" + returnStr +  ";" + inputFile + ";\r\n";
                await fs.appendFileSync(errorfile, payload);
                break;
            }				
            else {
                const payload = `${csn}` + ";" + netTypeStr + ";" + billTypeStr + ";" + returnStr +  ";" + inputFile + ";\r\n";
                await fs.appendFileSync(outfile, payload);
            }			
        } else if ((net_type === 11) & (bill_type === 1)) {
            [returnStr, isAnswerTimeInvalid, isCoversationTimeInvalid] = await fixed_ordinary(fd, csn, netTypeStr, billTypeStr);
            if (isAnswerTimeInvalid | isCoversationTimeInvalid) {
                const payload = `${csn}` + ";" + netTypeStr + ";" + billTypeStr + ";" + returnStr +  ";" + inputFile + ";\r\n";
                await fs.appendFileSync(errorfile, payload);
                break;
            }
            else {
                const payload = `${csn}` + ";" + netTypeStr + ";" + billTypeStr + ";" + returnStr +  ";" + inputFile + ";\r\n";
                await fs.appendFileSync(outfile, payload);
            }
        } else if ((net_type==11) & (bill_type==2)) {
            returnStr, isAnswerTimeInvalid, isCoversationTimeInvalid = await fixed_in(f, csn, netTypeStr, billTypeStr);
            if (isAnswerTimeInvalid | isCoversationTimeInvalid) {
                const payload = `${csn}` + ";" + netTypeStr + ";" + billTypeStr + ";" + returnStr +  ";" + inputFile + ";\r\n";
                await fs.appendFileSync(errorfile, payload);
                break;
            }
            else {
                const payload = `${csn}` + ";" + netTypeStr + ";" + billTypeStr + ";" + returnStr +  ";" + inputFile + ";\r\n";
                await fs.appendFileSync(outfile, payload);
            }	
        } else {
            const payload = `${csn}` + ";" + netTypeStr + ";" + billTypeStr + ";" + returnStr +  ";" + inputFile + ";\r\n";
            await fs.appendFileSync(errorfile, payload);
            break;
        }
    }
}

(async function main () {
    try {
        if (os.endianness() === 'LE') {
            // intel, alpha
            console.log("Little-endian platform.");
        } else {
            // motorola, sparc
            console.log("Big-endian platform.")
        }
        let script_path = __filename;
        let script_dir = __dirname;
        let inputdir = script_dir;

        let outputfile = path.join(script_dir, outfile);  
        console.log("Start Processing")
        
        await fs.appendFileSync(outfile, "CSN;NetType;BillType;AnswerTime;ConversationEndTime;CallDuration;CallerNumber;CalledNumber;ContactedNumber;FileName;\r\n");
        
        let novos_dir = path.join(inputdir, 'NOVOS');
        if (!fs.existsSync(novos_dir)){
            console.log("no dir NOVOS, Create a dir called NOVOS and put all CDRs inside.");
            return;
        }
    
        const files=fs.readdirSync(novos_dir);

        for(let file of files) {
            let filename = path.join(novos_dir, file);
            let stat = fs.lstatSync(filename);
            if (!stat.isDirectory() && /\.dat$/.test(filename)){
                await processFile(filename);
            }
        };

        console.log("Finished Processing");
    } catch (err) {
        console.log(err);
        process.exit(0);
    }
})();
