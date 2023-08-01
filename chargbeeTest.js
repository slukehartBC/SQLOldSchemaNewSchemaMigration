// const chargebee = require("chargebee");
/* eslint-disable */
const {Pool} = require("pg");
const jwt = require("jsonwebtoken");
const axios = require("axios");
const fs = require("fs");
const Buffer = require('buffer/').Buffer
const url = "https://tableau.bearcognition.com/api/3.16/sites/"
const dataArray = [];
const day = new Date().getDate();
const year = new Date().getFullYear()
const month = new Date().getMonth();
const time = new Date().getUTCFullYear()
const daUtC = new Date().getUTCMonth()
const aDate = new Date().getUTCDate() + "" + new Date().getUTCHours();



// eslint-disable-next-line max-len


const port = 5432;

const pool = new Pool({
    user: postgresUser,
    host: postgresHost,
    database: postgresDatabase,
    password: postgresPassword,
    port: port,
    statement_timeout: 10000,
});


function getTableauToken() {
    const issuer = tableauIssuer;
    return jwt.sign(
        {
            scp: ["tableau:views:embed",
                "tableau:content:read",
                "tableau:workbooks:download"],
        },
        tableauSecretValue,
        {
            algorithm: "HS256",
            header: {
                kid: tableauSecretKey,
                iss: issuer,
                alg: "HS256",
                typ: "JWT",
            },
            issuer: issuer,
            audience: "tableau",
            subject: tableauUserName,
            noTimestamp: true,
            expiresIn: 600,
            notBefore: "5s",
            jwtid: new Date().getTime().toString(),
        },
    );
}

async function signInToTableau() {
    const jwtToken = getTableauToken();

    // Create the request body
    const reqBody = {
        credentials: {
            jwt: jwtToken,
            site: {
                contentUrl: "",
            },
        },
    };

    // Create the request options
    const requestOptions = {
        headers: {
            "content-type": "application/json",
        },
        responseType: "json",
    };

    // Make the request
    const response = await axios.post(
        "https://tableau.bearcognition.com/api/3.16/auth/signin",
        reqBody, requestOptions,
    )
        .catch(function (error) {
            console.log("Error in Tableau Sign-in");
            console.log(error.code);
            console.log(error.response.data);
        });

    // Read the response
    const responseObj = await response;
    const responseJson = responseObj.data;

    const authToken = responseJson.credentials.token;
    const siteId = responseJson.credentials.site.id;
    const userId = responseJson.credentials.user.id;

    return {
        token: authToken,
        siteId: siteId,
        userId: userId,
    };
}

async function generateDetails() {

    try {


        const orgReportQuery = 'select * from org_report';

        const responseOrgReport = await pool.query(orgReportQuery);

        const orgReportsRows = responseOrgReport.rows;

        const orgOwner = 'select * from user_org_role where user_org_role.roleid = 4';
        const orgOwnerResponse = await pool.query(orgOwner);
        const reportIdMap = new Map();
        await Promise.all(orgOwnerResponse.rows.map(async (row) => {
            const query = `select reportid
                           from org_report
                           where org_report.organizationid = ${row.organizationid}`;
            const reportIdResponse = await pool.query(query);
            if (reportIdResponse.rows.length > 0) {
                reportIdMap.set(row.userid, reportIdResponse.rows);
            }
        }));

        let count = 0;

        const userDataPromises = [...reportIdMap.keys()].map(async (key) => {
            const userQuery = `select *
                               from app_user
                               where app_user.userid = ${key}`;
            const userResponse = await pool.query(userQuery);
            return userResponse.rows;
        });

        const userData = await Promise.all(userDataPromises);

        const reportDataPromises = [...reportIdMap.values()].map(async (val) => {
            return Promise.all(
                val.map(async (value) => {
                    try {
                        const reportQuery = `SELECT *
                                             FROM report
                                             WHERE report.reportid = ${value.reportid}`;
                        const reportResponse = await pool.query(reportQuery);
                        return reportResponse.rows;
                    } catch (e) {
                        console.log(e);
                        // Return an empty array or handle the error as needed
                        return [];
                    }
                })
            );
        });

        const reportData = await Promise.all(reportDataPromises);

        const flatReportdata = [];
        for (let i = 0; i < reportData.length; i++) {


            flatReportdata.push(reportData[i].flat(1))


        }
        const mutatedReportPromise = flatReportdata.map(async (val) => {

            if (val.length > 1) {
                return Promise.all(
                    val.map(async (alteredValue) => {
                        try {
                            const metaDataQuery = `SELECT reportidentifier
                                                   FROM report_metadata
                                                   WHERE reportid = ${alteredValue.reportid}`;
                            const metaDataResponse = await pool.query(metaDataQuery);
                            if (metaDataResponse.rows.length > 0 && metaDataResponse.rows) {
                                alteredValue.reportpathidentifier = metaDataResponse.rows[0].reportidentifier;
                            } else {
                                const urlQuery = `SELECT reporturl
                                                  FROM report_custom
                                                  WHERE reportid = ${alteredValue.reportid}`;
                                const urlResponse = await pool.query(urlQuery);
                                alteredValue.reportpathidentifier = urlResponse.rows[0].reporturl;
                            }

                            const reportTypeQuery = `SELECT typeenum
                                                     FROM report_type
                                                     WHERE reporttypeid = ${alteredValue.reporttypeid}`;
                            const reportTypeResponse = await pool.query(reportTypeQuery);
                            alteredValue.reporttypeid = reportTypeResponse.rows[0].typeenum;

                            return alteredValue;
                        } catch (e) {
                            console.log(`Error occurred at ${alteredValue}`);
                            console.log(e);
                            // Handle the error if necessary
                            return null; // Return null or a default value in case of an error
                        }
                    })
                );
            } else {
                try {
                    const metadataQuery = `select reportidentifier
                                           from report_metadata
                                           where reportid = ${val[0].reportid}`
                    const metaDataResponse = await pool.query(metadataQuery);
                    if (metaDataResponse.rows.length > 0 && metaDataResponse.rows) {
                        val[0].reportpathidentifier = metaDataResponse.rows[0].reportidentifier
                    } else {
                        const urlQuery = `select reporturl
                                          from report_custom
                                          where reportid = ${val[0].reportid}`

                        const urlResponse = await pool.query(urlQuery);
                        val[0].reportpathidentifier = urlResponse.rows[0].reporturl

                    }
                    const reportTypeQuery = `select typeenum
                                             from report_type
                                             where reporttypeid = ${val[0].reporttypeid}`
                    const reportTypeResponse = await pool.query(reportTypeQuery);
                    val[0].reporttypeid = reportTypeResponse.rows[0].typeenum;
                } catch (e) {
                    console.log(`Error occurred at ${val}`);
                    console.log(e);

                }
                return val;
            }
        });
        const mutatedReportArray = await Promise.all(mutatedReportPromise);


        //retrieve tableau thumbnailId
        const regex = /^(?!http:\/\/|https:\/\/)[a-fA-F0-9]{8}-(?:[a-fA-F0-9]{4}-){3}[a-fA-F0-9]{12}$/;

        const aMutatedPromise = mutatedReportArray.map(async (value) => {
            if (value.length > 1) {
                return Promise.all(value.map(async (val) => {
                    if (val.reporttypeid === 'TBL' && regex.test(val.reportpathidentifier)) {
                        let tableauViewId = await getTableauDetails(val.reportpathidentifier);
                        val.thumbnailpath = tableauViewId.defaultViewId;
                        val.updateddate = tableauViewId.updatedAt;
                        return val;

                    } else {
                        return val

                    }
                }));
            } else {
                if (value[0].reporttypeid === 'TBL' && regex.test(value[0].reportpathidentifier)) {
                    //console.log(mutatedReportArray[i].reportpathidentifier)

                    let tableauViewId = await getTableauDetails(value[0].reportpathidentifier);
                    value[0].thumbnailpath = tableauViewId.defaultViewId;
                    value[0].updateddate = tableauViewId.updatedAt;
                    return value[0];

                } else {
                    return value[0];
                }
            }
        })
        const updatedMutatedReportArray = await Promise.all(aMutatedPromise);

        const flat = userData.flat(1);
        const nameData = new Map();
        let nameCount = 0;
        flat.map((value) => {

            const displayName = value.displayname;
            const displayNameArray = displayName.split(" ");
            const firstName = displayNameArray[0] + nameCount;
            const lastName = displayNameArray[1] + nameCount;
            nameData.set(firstName, lastName);
            nameCount++
        })

        let dataCount = 0;
        for ([key, val] of nameData) {

            const responseObj = {
                firstName: key,
                lastName: val,
                userId: flat[dataCount].userid,
                phoneNumber: generateRandomPhoneNumber(),
                lastLoginTime: month + "-" + day + "-" + year,
                is_verified: true,
                role_id: 'account_owner',
                lastModified: month + "-" + day + "-" + year,
                isFirstTime: false,
                firebaseId: '',
                reportReportArray: updatedMutatedReportArray[dataCount],
                addedDate: month + "-" + day + "-" + year,

            };
            dataArray.push(responseObj);
            dataCount++;
        }

        for (let i = 0; i < dataArray.length; i++) {
            let firebaseIdQuery = `select uid
                                   from user_auth_data
                                   where userid = ${dataArray[i].userId}`
            let firebaseIdResponse = await pool.query(firebaseIdQuery);
            dataArray[i].firebaseId = firebaseIdResponse.rows[0].uid;

        }
        return dataArray;


    } catch (err) {
        console.log("[Error] fetching the user - ", err);
    }
}

async function getOrgInfo() {
    let obj = await generateDetails()

    let responseObj = obj.map(async (val) => {
        try {
            const orgQuery = `SELECT *
                              FROM organization
                              WHERE organizationid IN (SELECT organizationid
                                                       FROM user_org_role
                                                       WHERE userid = ${val.userId});`;
            const orgResponse = await pool.query(orgQuery);
            val.orgInfo = orgResponse.rows[0];
            const orgChargebeeQuery = `select *
                                       from org_subscription
                                       where ${val.orgInfo.organizationid} = organizationid`
            const chargebeeInfo = await pool.query(orgChargebeeQuery);
            if (chargebeeInfo.rows.length > 0) {

                val.orgInfo.chargebee_subscription_id = chargebeeInfo.rows[0].subscriptionid;
                val.orgInfo.next_invoice_date = chargebeeInfo.rows[0].nextinvoicedate;
            } else {
                val.orgInfo.chargebee_subscription_id = null;
                val.orgInfo.next_invoice_date = null;
            }
            const orgBillingQuery = `select billingenum
                                     from billing_option
                                     where ${val.orgInfo.billingoptionid} = billingoptionid`
            const billingInfo = await pool.query(orgBillingQuery);
            val.orgInfo.billingoptionid = billingInfo.rows[0].billingenum;
            val.orgInfo.organizationid = generateFnumberOrgId(orgResponse.rows[0].organizationid);

            return val

        } catch (e) {

            console.log(e);
        }


    })
    return await Promise.all(responseObj);
}

async function insertData() {
    const dataObj = await getOrgInfo();
    console.log(dataObj)
    const client = await pool.connect();

    try {
        for (const value of dataObj) {

            try {
                await client.query('BEGIN');
                const billingInfoQuery = `
                    SELECT id
                    FROM test_db_update.billing_plan
                    WHERE name = $1
                `;

                const billingInfoResponse = await client.query(billingInfoQuery, [value.orgInfo.billingoptionid]);
                const billingInfoId = billingInfoResponse.rows[0].id
                const isTrial = value.orgInfo.billingoptionid === 'trial';
                const fivetran_groupid = value.orgInfo.fivetran_groupid ? value.orgInfo.fivetran_groupid : 'overjoyed_lottery';
                const orgInsertQuery = `
                    INSERT INTO test_db_update.organization(f_number_id, name, phone_number, fivetran_groupid,
                                                            billing_plan_id,
                                                            is_trial, is_enabled, chargebee_subscription_id,
                                                            next_invoice_date, created_date)
                    VALUES ($1, $2, $3, $4, $5, $6, true, $7, $8, $9)
                    RETURNING id, f_number_id, name, phone_number, fivetran_groupid,
                        billing_plan_id,
                        is_trial, is_enabled, chargebee_subscription_id,
                        next_invoice_date, created_date
                `;

                // Pass values as an array in the same order as the placeholders in the query
                const orgResult = await client.query(orgInsertQuery, [
                    value.orgInfo.organizationid,
                    value.orgInfo.companyname,
                    value.orgInfo.contactnumber,
                    fivetran_groupid,
                    billingInfoId,
                    isTrial,
                    value.orgInfo.chargebee_subscription_id,
                    value.orgInfo.next_invoice_date,
                    value.orgInfo.createddate,
                ]);

                const organizationId = orgResult.rows[0].id;
                console.log("Organization ID" + organizationId);
                const userRoleQuery = `
                    SELECT id
                    FROM test_db_update.user_role
                    WHERE role_enum = 'account_owner'
                `;
                const userRoleResponse = await client.query(userRoleQuery);
                const roleId = userRoleResponse.rows[0].id;


                const userInsertQuery = `
                    INSERT INTO test_db_update."user" (first_name, last_name, phone_number, organization_id,
                                                       is_verified, role_id, is_first_time, firebase_id)
                    VALUES ($1, $2, $3, $4, true, $5, false, $6)
                    RETURNING id
                `;

                const userResult = await client.query(userInsertQuery, [
                    value.firstName,
                    value.lastName,
                    value.phoneNumber,
                    organizationId,
                    roleId,
                    value.firebaseId,
                ]);

                const userId = userResult.rows[0].id;
                console.log("User ID " + userId)

                // Add organizationId to the userResult object
                const resultObj = {}


                const orgUpdateQuery = `
                    UPDATE test_db_update.organization
                    SET org_owner_id = $1
                    WHERE id = $2;
                `;

                await client.query(orgUpdateQuery, [userId, organizationId]);

                if (Array.isArray(value.reportReportArray)) {
                    for(const val of value.reportReportArray){
                        await client.query('BEGIN');


                        try {
                            const productInsertQuery = `INSERT INTO test_db_update.product (name, added_date, product_type, organization_id)
                                                        values ($1, $2, $3, $4)
                                                        returning id`
                            const productResult = await client.query(productInsertQuery, [val.reportname, val.createddate, val.reporttypeid, organizationId]);
                            const productId = productResult.rows[0].id;
                            const insertProductUserQuery = `insert into test_db_update.product_user(user_id, product_id)
                                                            values ($1, $2)`
                            const productUserResult = await client.query(insertProductUserQuery, [userId, productId]);
                            if (val.reporttypeid === 'TBL') {
                                if (val.hasOwnProperty('updateddate')) {
                                    const insertTableauPrduct = `insert into test_db_update.tableau_embed_product(product_id, tableau_id, thumbnail_id, is_custom, updated_date)
                                                                 values ($1, $2,
                                                                         $3, $4, $5)`
                                    const tableauInsertResponse = await client.query(insertTableauPrduct, [productId, val.reportpathidentifier, val.thumbnailpath, val.iscustomreport, val.updateddate]);
                                } else {
                                    const insertTableauProduct = `insert into test_db_update.tableau_embed_product(product_id, tableau_id, thumbnail_id, is_custom)
                                                                 values ($1, $2, $3, $4)`
                                    const tableauInsertResponse = await client.query(insertTableauProduct, [productId, val.reportpathidentifier, val.thumbnailpath, val.iscustomreport]);
                                }


                            } else if (val.reporttypeid === 'AWS_QS') {
                                const interAwsQuery = `insert into test_db_update.awsquicksight_product(product_id, quicksight_url, thumbnail_url)
                                                       values ($1, $2, $3)`
                                const awsResponse = await client.query(interAwsQuery, [productId, val.reportpathidentifier, val.thumbnailpath]);

                            } else if (val.reporttypeid === 'EMBED') {
                                const insertCustomEmbedQeury = `insert into test_db_update.custom_embedded_product(product_id, embed_url, thumbnail_url)
                                                                values ($1, $2, $3)`
                                const customEmbedResponse = await client.query(insertCustomEmbedQeury, [productId, val.reportpathidentifier, val.thumbnailpath]);
                                await client.query('COMMIT');

                            }
                        } catch (e) {
                            await client.query('ROLLBACK');
                            console.log('inside second mapping')
                            console.log('Data insertion failed in report loop' + e);
                        }

                    }

                } else {
                    const productInsertQuery = `
                        INSERT INTO test_db_update.product (name, added_date, product_type, organization_id)
                        VALUES ($1, $2, $3, $4)
                        returning id
                    `;
                    console.log(value.reportReportArray.reportname,
                        value.reportReportArray.createddate,
                        value.reportReportArray.reporttypeid,
                        organizationId,)
                    const productResult = await client.query(productInsertQuery, [
                        value.reportReportArray.reportname,
                        value.reportReportArray.createddate,
                        value.reportReportArray.reporttypeid,
                        organizationId,
                    ]);
                    const productId = productResult.rows[0].id;
                    const insertProductUserQuery = `
                        INSERT INTO test_db_update.product_user (user_id, product_id, is_favorited)
                        VALUES ($1, $2, $3)
                    `;

                    const productUserResult = await client.query(insertProductUserQuery, [
                        userId,
                        productId,
                        false,
                    ]);
                    if (value.reporttypeid === 'TBL') {
                        if (value.hasOwnProperty('updateddate')) {
                            const insertTableauPrduct = `
                                INSERT INTO test_db_update.tableau_embed_product (product_id, tableau_id, thumbnail_id, is_custom, updated_date)
                                VALUES ($1, $2, $3, $4, $5)`;

                            const tableauInsertResponse = await client.query(insertTableauPrduct, [
                                productId,
                                value.reportReportArray.reportpathidentifier,
                                value.reportReportArray.thumbnailpath,
                                value.reportReportArray.iscustomreport,
                                value.reportReportArray.updateddate,
                            ]);

                        } else {
                            const insertTableauPrduct = `
                                INSERT INTO test_db_update.tableau_embed_product (product_id, tableau_id, thumbnail_id, is_custom)
                                VALUES ($1, $2, $3, $4)`;

                            const tableauInsertResponse = await client.query(insertTableauPrduct, [
                                productId,
                                value.reportReportArray.reportpathidentifier,
                                value.reportReportArray.thumbnailpath,
                                value.reportReportArray.iscustomreport,
                            ]);
                        }


                    } else if (value.reporttypeid === 'AWS_QS') {
                        const interAwsQuery = `
                            INSERT INTO test_db_update.awsquicksight_product (product_id, quicksight_url, thumbnail_url)
                            VALUES ($1, $2, $3)
                        `;

                        const awsResponse = await client.query(interAwsQuery, [
                            productId,
                            value.reportReportArray.reportpathidentifier,
                            value.reportReportArray.thumbnailpath,
                        ]);

                    } else if (value.reporttypeid === 'EMBED') {
                        const insertCustomEmbedQeury = `
                            INSERT INTO test_db_update.custom_embedded_product (product_id, embed_url, thumbnail_url)
                            VALUES ($1, $2, $3)
                        `;

                        const customEmbedResponse = await client.query(insertCustomEmbedQeury, [
                            productId,
                            value.reportReportArray.reportpathidentifier,
                            value.reportReportArray.thumbnailpath,
                        ]);
                    }
                }


                await client.query('COMMIT');
            } catch (e) {
                console.log(e);
                await client.query('ROLLBACK');

            }


        }

    } catch (e) {
        console.log("outside of iteration" + e)
    }finally {
        client.release();
    }


}

function generateFnumberOrgId(orgId) {

    const orgIdToF = orgId.toString();
    let zeroString = '000000';
    const numLen = orgIdToF.length;

    const resLen = zeroString.length;

    let index = 1;
    let carry = 0;

    while (index <= numLen) {
        const num = parseInt(orgIdToF[numLen - index]);
        const res = parseInt(zeroString[resLen - index]);

        const total = num + res + carry;
        zeroString =
            zeroString.slice(0, resLen - index) +
            (total % 10).toString() +
            zeroString.slice(resLen - index + 1);

        carry = Math.floor(total / 10);

        index++;
    }

    if (carry > 0) {
        zeroString = carry.toString() + zeroString;
    }


    return "F" + zeroString;
}

function getColumnNames(errorMessage) {
    const regex = /column "(.*?)"/g;
    const matches = [];
    let match;

    while ((match = regex.exec(errorMessage)) !== null) {
        matches.push(match[1]);
    }

    return matches;
}

// eslint-disable-next-line require-jsdoc,no-unused-vars
function generateRandomPhoneNumber() {
    // Generate a random 10-digit number
    const randomNum = Math.floor(1000000000 + Math.random() * 9000000000);

    // Format the number as a phone number
    const phoneNumber = String(randomNum).padStart(10, '0');
    // eslint-disable-next-line max-len
    return `${phoneNumber.slice(0, 3)}-${phoneNumber.slice(3, 6)}-${phoneNumber.slice(6)}`;
}


async function getTableauDetails(id) {
    const authDetails = await signInToTableau();
    const requestOptions = {
        headers: {
            "content-type": "application/json",
            "X-Tableau-Auth": authDetails.token,
        },
        responseType: "json",
    };

    const reqUrl = url + authDetails.siteId + "/workbooks/" + id;
    try {
        let response = await axios.get(reqUrl, requestOptions)
        return response.data.workbook


    } catch (e) {
        console.log(e)
    }


}

async function getTblEncoding(id) {
    const authDetails = await signInToTableau();
    const requestOptions = {
        headers: {
            "content-type": "application/json",
            "X-Tableau-Auth": authDetails.token,
        },
        responseType: "arraybuffer",
    };
    const url = "https://tableau.bearcognition.com/api/3.16/sites/" +
        authDetails.siteId + "/views/" + id + "/image?resolution=high";


    try {
        let response = await axios.get(url, requestOptions)
        const data = Buffer.from(response.data, "binary").toString("base64");
        console.log()
        return "data:image/png;base64," + data;


        // remove the image header


    } catch (error) {
        console.log("Error in Tableau Thumbnail API");
        console.log(error.code);
        console.log(error.response.data);
        console.log(Buffer.from(error.response.data, "binary").toString("base64"));

    }
}

insertData().then((r) => console.log("success")).catch((e) => console.log(e));

