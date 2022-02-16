import dotenv from 'dotenv';
import redis from 'redis';
import cron from 'cron';
import arango from 'arangojs';

// Environment Variables
dotenv.config();
const { Database, aql } = arango;
// ArangoDB
const dbInstance = new Database({
    url:"http://127.0.0.1:8529"
});
const PacoDB = dbInstance.useBasicAuth(process.env.ARANGODB_USER, process.env.ARANGODB_KEY).useDatabase('paco');
const PacoStore = PacoDB.collection('account');
const PacoStoreBoxx = PacoDB.collection('store_boxx');
const PacoStoreBrick = PacoDB.collection('store_brick');
const PacoStoreProduct = PacoDB.collection('store_product');

const BoxxClient = redis.createClient();
const BrickClient = redis.createClient();
const ProductClient = redis.createClient();

BoxxClient.on('error', (error) => console.error.bind(console, error));

BoxxClient.once('connect', async () => {
    console.log("Connected To Redis Boxx!");
    BoxxClient.select(0);
    BoxxClient.FLUSHDB( async (err, done)=>{
        if (err) console.log(err);
        console.log(`Flushed: ${done} Boxx`);
        await CronEngine.BoxxStack();
    });
});

BrickClient.on('error', (error) => console.error.bind(console, error));

BrickClient.once('connect', async () => {
    console.log("Connected To Redis Brick!");
    BrickClient.select(1);
    BrickClient.FLUSHDB( async (err, done)=>{
        if (err) console.log(err);
        console.log(`Flushed: ${done} Brick`);
        await CronEngine.BrickStack();
    });
});

ProductClient.on('error', (error) => console.error.bind(console, error));

ProductClient.once('connect', async () => {
    console.log("Connected To Redis Product!");
    ProductClient.select(2);
    ProductClient.FLUSHDB( async (err, done)=>{
        if (err) console.log(err);
        console.log(`Flushed: ${done} Product`);
        await CronEngine.ProductStack();
    });
    // job.start();
});

const job = new cron.CronJob('*/30 * * * * *', async () => {
    try { 
        await CronEngine.BoxxStack();
        await CronEngine.BrickStack();
        await CronEngine.ProductStack();
        BoxxClient.SMEMBERS("boxx", async (err, data)=>{
            if (err) console.log(err);
            data.forEach((key) => {
                let now = Date.now();
                BoxxClient.hgetall(`${key}`, async (err, boxx)=>{
                    if (err) console.log(err);
                    await CronEngine.BoxxOnOff(now, key, boxx.stamp_on, boxx.stamp_off, boxx.owner, boxx.timed);
                });
            });
        });
        BrickClient.SMEMBERS("brick", async (err, data)=>{
            if (err) console.log(err);
            data.forEach((key) => {
                let now = Date.now();
                BrickClient.hgetall(`${key}`, async (err, brick)=>{
                    if (err) console.log(err);
                    await CronEngine.BrickOnOff(now, key, brick.parent, brick.stamp_on, brick.stamp_off, brick.owner, brick.timed);
                });
            });
        });
        ProductClient.SMEMBERS("product", async (err, data)=>{
            if (err) console.log(err);
            data.forEach((key) => {
                let now = Date.now();
                ProductClient.hgetall(`${key}`, async (err, product)=>{
                    if (err) console.log(err);
                    await CronEngine.ProductOnOff(now, key, product.brick, product.parent, product.stamp_on, product.stamp_off, product.owner, product.timed);
                });
            });
        });
    } catch (error) {
        console.log(`CronStack: ${error}`);
    }
}, null, true, 'Africa/Lagos');

export default class CronEngine {
    static async BoxxStack() {
        try {   
            const Boxx = await PacoDB.query(aql`
                FOR boxx IN store_boxx
                FILTER boxx.options.timed == true
                RETURN { owner:boxx.owner, id:boxx._key, timed:boxx.options.timed, stamp_on:boxx.options.time.on.stamp, stamp_off:boxx.options.time.off.stamp }
            `);
            const { _result } = Boxx;
            _result.forEach((boxx)=>{
                BoxxClient.HMSET(boxx.id, { owner:boxx.owner, stamp_on:boxx.stamp_on, stamp_off:boxx.stamp_off, timed:boxx.timed }, (err, _done)=>{
                    if (err) console.log(err);
                    else {
                        BoxxClient.SADD("boxx", boxx.id, (err, _done) => {
                            if (err) console.log(err);
                            console.log("Set: "+ boxx.id + " Boxx");
                        });
                    }
                });
            });
        } catch (error) {
            console.log(`BoxxStack: ${error}`);
        }
    }
    static async BrickStack() {
        try {
            const Brick = await PacoDB.query(aql`
                FOR brick IN store_brick
                FILTER brick.options.timed == true
                RETURN { owner:brick.owner, id:brick._key, parent:brick.parent, timed:brick.options.timed, stamp_on:brick.options.time.on.stamp, stamp_off:brick.options.time.off.stamp }
            `);
            const { _result } = Brick;
            _result.forEach((brick)=>{
                BrickClient.HMSET(brick.id, { owner:brick.owner, parent:brick.parent, stamp_on:brick.stamp_on, stamp_off:brick.stamp_off, timed:brick.timed }, (err, _done)=>{
                    if (err) console.log(err);
                    else {
                        BrickClient.SADD("brick", brick.id, (err, _done) => {
                            if (err) console.log(err);
                            console.log("Set: "+ brick.id + " Brick");
                        });
                    }
                });
            });
        } catch (error) {
            console.log(`BrickStack: ${error}`);
        }
    }
    static async ProductStack() {
        try {
            const Product = await PacoDB.query(aql`
                FOR product IN store_product
                FILTER product.options.timed == true
                RETURN { owner:product.owner, brick:product.brick, id:product._key, parent:product.parent, timed:product.options.timed, stamp_on:product.options.time.on.stamp, stamp_off:product.options.time.off.stamp }
            `);
            const { _result } = Product;
            _result.forEach((product)=>{
                ProductClient.HMSET(product.id, { owner:product.owner, brick:product.brick, parent:product.parent, stamp_on:product.stamp_on, stamp_off:product.stamp_off, timed:product.timed }, (err, _done)=>{
                    if (err) console.log(err);
                    else {
                        ProductClient.SADD("product", product.id, (err, _done) => {
                            if (err) console.log(err);
                            console.log("Set: "+ product.id + " Product");
                        });
                    }
                });
            });
        } catch (error) {
            console.log(`ProductStack: ${error}`);
        }
    }
    // Boxx Start
    BoxxOnOffStack(owner, id, stamp_on, stamp_off, timed) {
        try {  
            const saved = BoxxClient.HMSET(id, { owner, stamp_on, stamp_off, timed }, (err, done)=>{
                if (err) console.log(err);
                else console.log(done);
                console.log(done, stamp_on, stamp_off);
            });
            return saved;
        } catch (error) {
            console.log(`BoxxOnOffStack: ${error}`);
        }
    }
    static async BoxxOnOff(now ,id, stamp_on, stamp_off, owner, timed) {
        try {
            if (parseInt(stamp_on) < now && stamp_off === "00" && timed === "true") {
                const Boxx = await PacoStoreBoxx.updateByExample({ owner, _key:id }, { 
                    live: true,
                    options: {
                        timed: false,
                        time: {
                            on: {
                                stamp: "00",
                                time: {
                                    hour: "00",
                                    minutes: "00",
                                    seconds: "00"
                                },
                                moment: {
                                    day: "00",
                                    month: "00",
                                    year: "00"
                                }
                            },
                            off: {
                                stamp: "00",
                                time: {
                                    hour: "00",
                                    minutes: "00",
                                    seconds: "00"
                                },
                                moment: {
                                    day: "00",
                                    month: "00",
                                    year: "00"
                                }
                            }
                        }
                    }
                }, {
                    waitForSync: true
                });
                console.log("I did It");
                BoxxClient.SREM("boxx", id, (err, _done)=>{
                    if (err) console.log(err);
                    else {
                        BoxxClient.HDEL(id, "stamp_on", "stamp_off", "owner", "timed", (err, done)=> {
                            if (err) return console.log(err);
                            console.log("Removed: "+done);
                        });
                    } 
                });
                if (!Boxx) return false;
                return true;
            }else if (parseInt(stamp_on) < now && stamp_off !== "00") {
                const one_day = 86400000;
                console.log("Entered Here");
                if(parseInt(stamp_off) < now) {
                    const Boxx = await PacoStoreBoxx.updateByExample({ owner, _key:id }, { 
                        live: false,
                        options: {
                            timed: true,
                            time: {
                                off: {
                                    stamp: parseInt(stamp_off) + one_day,
                                    moment: {
                                        day: "00",
                                        month: "00",
                                        year: "0000"
                                    }
                                }
                            }
                        }
                    }, {
                        waitForSync: true
                    });
                    if (!Boxx){
                        BoxxClient.SREM("boxx", id, (err, _done)=>{
                            if (err) console.log(err);
                            else {
                                BoxxClient.HDEL(id, "stamp_on", "stamp_off", "owner", "timed", (err, done)=> {
                                    if (err) return console.log(err);
                                    console.log("Removed: "+done);
                                });
                            } 
                        });
                        return  { done:false, message:`💥Boxx Does Not Exist!`, account:"<@store"};
                    } 
                    console.log("Yep");
                }else if(parseInt(stamp_on) < now) {
                    const Boxx = await PacoStoreBoxx.updateByExample({ owner, _key:id }, { 
                        live: true,
                        options: {
                            timed: true,
                            time: {
                                on: {
                                    stamp: parseInt(stamp_on) + one_day,
                                    moment: {
                                        day: "00",
                                        month: "00",
                                        year: "0000"
                                    }
                                }
                            }
                        }
                    }, {
                        waitForSync: true
                    });
                    if (!Boxx){
                        BoxxClient.SREM("boxx", id, (err, _done)=>{
                            if (err) console.log(err);
                            else {
                                BoxxClient.HDEL(id, "stamp_on", "stamp_off", "owner", "timed", (err, done)=> {
                                    if (err) return console.log(err);
                                    console.log("Removed: "+done);
                                });
                            } 
                        });
                        return  { done:false, message:`💥Boxx Does Not Exist!`, account:"<@store"};
                    } 
                    console.log("Yepa");
                }
            }else if(parseInt(stamp_off) < now && stamp_off !== "00") {
                const one_day = 86400000;
                const Boxx = await PacoStoreBoxx.updateByExample({ owner, _key:id }, { 
                    live: false,
                    options: {
                        timed: true,
                        time: {
                            off: {
                                stamp: parseInt(stamp_off) + one_day,
                                moment: {
                                    day: "00",
                                    month: "00",
                                    year: "0000"
                                }
                            }
                        }
                    }
                }, {
                    waitForSync: true
                });
                if (!Boxx){
                    BoxxClient.SREM("boxx", id, (err, _done)=>{
                        if (err) console.log(err);
                        else {
                            BoxxClient.HDEL(id, "stamp_on", "stamp_off", "owner", "timed", (err, done)=> {
                                if (err) return console.log(err);
                                console.log("Removed: "+done);
                            });
                        } 
                    });
                    return  { done:false, message:`💥Boxx Does Not Exist!`, account:"<@store"};
                } 
                console.log("Yep");
            }else console.log("No Match.");
        } catch (error) {
            console.log(`BoxxOnOff: ${error}`);
        }
    }
    PopBoxx(id) {
        try {
            BoxxClient.SREM("boxx", id, (err, _done)=>{
                if (err) console.log(err);
                else {
                    BoxxClient.HDEL(id, "stamp_on", "stamp_off", "owner", "timed", (err, done)=> {
                        if (err) return console.log(err);
                        console.log("Removed: "+done);
                    });
                } 
            });
        } catch (error) {
            console.log(`PopBoxx: ${error}`);
        }
    }
    // Brick Start
    BrickOnOffStack(owner, brick, parent, stamp_on, stamp_off, timed) {
        try {
            const saved = BrickClient.HMSET(brick, { owner, parent, stamp_on, stamp_off, timed }, (err, done)=>{
                if (err) console.log(err);
                else console.log(done);
                console.log(done, stamp_on, stamp_off);
            });
            return saved;
        } catch (error) {
            console.log(`BrickOnOffStack: ${error}`);
        }
    }
    static async BrickOnOff(now, brick, parent, stamp_on, stamp_off, owner, timed) {
        try {
            if (parseInt(stamp_on) < now && stamp_off === "00" && timed === "true") {
                const Brick = await PacoStoreBrick.updateByExample({ owner, _key:brick, parent }, { 
                    live: true,
                    options: {
                        timed: false,
                        time: {
                            on: {
                                stamp: "00",
                                time: {
                                    hour: "00",
                                    minutes: "00",
                                    seconds: "00"
                                },
                                moment: {
                                    day: "00",
                                    month: "00",
                                    year: "00"
                                }
                            },
                            off: {
                                stamp: "00",
                                time: {
                                    hour: "00",
                                    minutes: "00",
                                    seconds: "00"
                                },
                                moment: {
                                    day: "00",
                                    month: "00",
                                    year: "00"
                                }
                            }
                        }
                    }
                }, {
                    waitForSync: true
                });
                console.log("I did It");
                BrickClient.SREM("brick", brick, (err, _done)=>{
                    if (err) console.log(err);
                    else {
                        BrickClient.HDEL(brick, "parent", "stamp_on", "stamp_off", "owner", "timed", (err, done)=> {
                            if (err) return console.log(err);
                            console.log("Removed: "+done);
                        });
                    } 
                });
                if (!Brick) return false;
                return true;
            }else if (parseInt(stamp_on) < now && stamp_off !== "00") {
                const one_day = 86400000;
                console.log("Entered Brick");
                if(parseInt(stamp_off) < now) {
                    const Brick = await PacoStoreBrick.updateByExample({ owner, _key:brick, parent }, { 
                        live: false,
                        options: {
                            timed: true,
                            time: {
                                off: {
                                    stamp: (parseInt(stamp_off) + one_day),
                                    moment: {
                                        day: "00",
                                        month: "00",
                                        year: "0000"
                                    }
                                }
                            }
                        }
                    }, {
                        waitForSync: true
                    });
                    if (!Brick) {
                        BrickClient.SREM("brick", brick, (err, _done)=>{
                            if (err) console.log(err);
                            else {
                                BrickClient.HDEL(brick, "parent", "stamp_on", "stamp_off", "owner", "timed", (err, done)=> {
                                    if (err) return console.log(err);
                                    console.log("Removed: "+done);
                                });
                            } 
                        });
                        return  { done:false, message:`💥Brick Does Not Exist!`, account:"<@store" };
                    }
                    console.log("Yep Brick");
                }else if(parseInt(stamp_on) < now) {
                    console.log("First");
                    console.log("Stamp: "+parseInt(stamp_on)+"New :"+(parseInt(stamp_on) + one_day));
                    const Brick = await PacoStoreBrick.updateByExample({ owner, _key:brick, parent }, { 
                        live: true,
                        options: {
                            timed: true,
                            time: {
                                on: {
                                    stamp: (parseInt(stamp_on) + one_day),
                                    moment: {
                                        day: "00",
                                        month: "00",
                                        year: "0000"
                                    }
                                }
                            }
                        }
                    }, {
                        waitForSync: true
                    });
                    if (!Brick) {
                        BrickClient.SREM("brick", brick, (err, _done)=>{
                            if (err) console.log(err);
                            else {
                                BrickClient.HDEL(brick, "parent", "stamp_on", "stamp_off", "owner", "timed", (err, done)=> {
                                    if (err) return console.log(err);
                                    console.log("Removed: "+done);
                                });
                            } 
                        });
                        return  { done:false, message:`💥Brick Does Not Exist!`, account:"<@store" };
                    }
                    console.log("Yep On Brick");
                }
            }else if(parseInt(stamp_off) < now && stamp_off !== "00") {
                console.log("Second");
                const one_day = 86400000;
                console.log("Stamp: "+parseInt(stamp_off)+"New :"+parseInt(stamp_off) + one_day);
                const Brick = await PacoStoreBrick.updateByExample({ owner, _key:brick, parent }, { 
                    live: false,
                    options: {
                        timed: true,
                        time: {
                            off: {
                                stamp: (parseInt(stamp_off) + one_day),
                                moment: {
                                    day: "00",
                                    month: "00",
                                    year: "0000"
                                }
                            }
                        }
                    }
                }, {
                    waitForSync: true
                });
                if (!Brick) {
                    BrickClient.SREM("brick", brick, (err, _done)=>{
                        if (err) console.log(err);
                        else {
                            BrickClient.HDEL(brick, "parent", "stamp_on", "stamp_off", "owner", "timed", (err, done)=> {
                                if (err) return console.log(err);
                                console.log("Removed: "+done);
                            });
                        } 
                    });
                    return  { done:false, message:`💥Brick Does Not Exist!`, account:"<@store" };
                }
                console.log("Yep Off Brick");
            }else console.log("No Match.");
        } catch (error) {
            console.log(`BrickOnOff: ${error}`);
        }
    }
    PopBrick(brick) {
        try {     
            BrickClient.SREM("brick", brick, (err, _done)=>{
                if (err) console.log(err);
                else {
                    BrickClient.HDEL(brick, "parent", "stamp_on", "stamp_off", "owner", "timed", (err, done)=> {
                        if (err) return console.log(err);
                        console.log("Removed: "+done);
                    });
                } 
            });
        } catch (error) {
            console.log(`PopBrick: ${error}`);
        }
    }
    // Product Start
    ProductOnOffStack(owner, product, brick, parent, stamp_on, stamp_off, timed) {
        try {      
            const saved = ProductClient.HMSET(product, { owner, brick, parent, stamp_on, stamp_off, timed }, (err, done)=>{
                if (err) console.log(err);
                else console.log(done);
                console.log(done, stamp_on, stamp_off);
            });
            return saved;
        } catch (error) {
            console.log(`ProductOnOffStack: ${error}`);
        }
    }
    static async ProductOnOff(now, product, brick, parent, stamp_on, stamp_off, owner, timed) {
        try {
            if (parseInt(stamp_on) < now && stamp_off === "00" && timed === "true") {
                const Product = await PacoStoreProduct.updateByExample({ owner, _key:product, brick, parent }, { 
                    live: true,
                    options: {
                        timed: false,
                        time: {
                            on: {
                                stamp: "00",
                                time: {
                                    hour: "00",
                                    minutes: "00",
                                    seconds: "00"
                                },
                                moment: {
                                    day: "00",
                                    month: "00",
                                    year: "00"
                                }
                            },
                            off: {
                                stamp: "00",
                                time: {
                                    hour: "00",
                                    minutes: "00",
                                    seconds: "00"
                                },
                                moment: {
                                    day: "00",
                                    month: "00",
                                    year: "00"
                                }
                            }
                        }
                    }
                }, {
                    waitForSync: true
                });
                console.log("I did It");
                ProductClient.SREM("product", product, (err, _done)=>{
                    if (err) console.log(err);
                    else {
                        ProductClient.HDEL(product, "parent", "brick", "stamp_on", "stamp_off", "owner", "timed", (err, done)=> {
                            if (err) return console.log(err);
                            console.log("Removed: "+done);
                        });
                    } 
                });
                if (!Product) return false;
                return true;
            }else if (parseInt(stamp_on) < now && stamp_off !== "00") {
                const one_day = 86400000;
                console.log("Entered Product");
                console.log(stamp_on +" "+ now +" "+ stamp_off);
                if(parseInt(stamp_off) < now) {
                    const Product = await PacoStoreProduct.updateByExample({ owner, _key:product, brick, parent }, { 
                        live: false,
                        options: {
                            timed: true,
                            time: {
                                off: {
                                    stamp: (parseInt(stamp_off) + one_day),
                                    moment: {
                                        day: "00",
                                        month: "00",
                                        year: "0000"
                                    }
                                }
                            }
                        }
                    }, {
                        waitForSync: true
                    });
                    if (!Product) {
                        ProductClient.SREM("product", product, (err, _done)=>{
                            if (err) console.log(err);
                            else {
                                ProductClient.HDEL(product, "parent", "brick", "stamp_on", "stamp_off", "owner", "timed", (err, done)=> {
                                    if (err) return console.log(err);
                                    console.log("Removed: "+done);
                                });
                            } 
                        });
                        return  { done:false, message:`💥Product Does Not Exist!`, account:"<@store" };
                    }
                    console.log("Yep Product");
                }else if(parseInt(stamp_on) < now) {
                    console.log("First");
                    console.log("Stamp: "+parseInt(stamp_on)+"New :"+(parseInt(stamp_on) + one_day));
                    const Product = await PacoStoreProduct.updateByExample({ owner, _key:product, brick, parent }, { 
                        live: true,
                        options: {
                            timed: true,
                            time: {
                                on: {
                                    stamp: (parseInt(stamp_on) + one_day),
                                    moment: {
                                        day: "00",
                                        month: "00",
                                        year: "0000"
                                    }
                                }
                            }
                        }
                    }, {
                        waitForSync: true
                    });
                    if (!Product) {
                        ProductClient.SREM("product", product, (err, _done)=>{
                            if (err) console.log(err);
                            else {
                                ProductClient.HDEL(product, "parent", "brick", "stamp_on", "stamp_off", "owner", "timed", (err, done)=> {
                                    if (err) return console.log(err);
                                    console.log("Removed: "+done);
                                });
                            } 
                        });
                        return  { done:false, message:`💥Product Does Not Exist!`, account:"<@store" };
                    }
                    console.log("Yep On Product");
                }
            }else if(parseInt(stamp_off) < now && stamp_off !== "00") {
                console.log("Second");
                const one_day = 86400000;
                console.log("Stamp: "+parseInt(stamp_off)+"New :"+parseInt(stamp_off) + one_day);
                const Product = await PacoStoreProduct.updateByExample({ owner, _key:product, brick, parent }, { 
                    live: false,
                    options: {
                        timed: true,
                        time: {
                            off: {
                                stamp: (parseInt(stamp_off) + one_day),
                                moment: {
                                    day: "00",
                                    month: "00",
                                    year: "0000"
                                }
                            }
                        }
                    }
                }, {
                    waitForSync: true
                });
                if (!Product) {
                    ProductClient.SREM("product", product, (err, _done)=>{
                        if (err) console.log(err);
                        else {
                            ProductClient.HDEL(product, "parent", "brick", "stamp_on", "stamp_off", "owner", "timed", (err, done)=> {
                                if (err) return console.log(err);
                                console.log("Removed: "+done);
                            });
                        } 
                    });
                    return  { done:false, message:`💥Product Does Not Exist!`, account:"<@store" };
                }
                console.log("Yep Off Product");
            }else console.log("No Match.");
        } catch (error) {
            console.log(`ProductOnOff: ${error}`);
        }
    }
    PopProduct(product) {
        try { 
            ProductClient.SREM("product", product, (err, _done)=>{
                if (err) console.log(err);
                else {
                    ProductClient.HDEL(product, "parent", "brick", "stamp_on", "stamp_off", "owner", "timed", (err, done)=> {
                        if (err) return console.log(err);
                        console.log("Removed: "+done);
                    });
                } 
            });
        } catch (error) {
            console.log(`PopProduct: ${error}`);
        }
    }
}
