package main

import (
	"database/sql"
	"fmt"
	"github.com/beevik/etree"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"gopkg.in/gomail.v2"
)

const (
	//server   = "10.253.35.142"
	//port     = 3306
	//user     = "root"
	//password = "Csi@solar10"
	//dbname   = "uid"
	//dbString = "server=%s;port%d;database=%s;user id=%s;password=%s;encrypt=disable;connection timeout=300;dial timeout=300;"
	server   = "10.253.32.52"
	port     = 3306
	user     = "rosicky.gui"
	password = "rosicky.gui"
	database = "UNI_ID"
	dbString = "server=%s;port%d;database=%s;user id=%s;password=%s;encrypt=disable;connection timeout=300;dial timeout=300;"

	smtpServer = "10.0.8.27"
	smtpPort   = 25

	MailReport = "service.uid@csisolar.com"
	MailAdmin  = "billy.zhou@csisolar.com"
	MailKeming = "keming.zan@csisolar.com"

	soapURL = "http://csicn02hcmd1.csisolar.com:8131/sap/bc/srt/wsdl/flv_10002A111AD1/bndg_url/sap/bc/srt/rfc/sap/zweb_get_kostl/300/zweb_get_kostl/zweb_get_kostl?sap-client=300&sap-language=ZH"

	soap12UrlDev = "http://csicn02hcmd1.csisolar.com:8131/sap/bc/srt/rfc/sap/zweb_get_kostl/300/zweb_get_kostl/zweb_get_kostl?sap-client=300&sap-language=ZH"
	soap12UrlPrd = "http://csicn01hcmv1.csisolar.com:8161/sap/bc/srt/rfc/sap/zweb_get_kostl/600/zweb_get_kostl/zweb_get_kostl?sap-client=600&sap-language=ZH"

	soapReqBody = `<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:urn="urn:sap-com:document:sap:rfc:functions">
  <soap:Header/>
  <soap:Body>
     <urn:ZHR_GET_KOSTL_ZL>
        <ABRUD>%s</ABRUD>
        <!--Optional:-->
        <T_KOSTL>
           <!--Zero or more repetitions:-->
           <item>
              <MANDT></MANDT>
              <ZPERNRID></ZPERNRID>
              <KOSTL></KOSTL>
              <LTEXT></LTEXT>
              <KOSTLBUKRS></KOSTLBUKRS>
              <LTEXTBUTXT></LTEXTBUTXT>
              <BUKRS></BUKRS>
              <BUTXT></BUTXT>
              <AEDTM></AEDTM>
              <ABRUD></ABRUD>
           </item>
        </T_KOSTL>
     </urn:ZHR_GET_KOSTL_ZL>
  </soap:Body>
</soap:Envelope>`

	soapUsername = "HCMOA"
	soapPassword = "Csi@2019"

	timeLayoutStr = "2006-01-02"
)

type CostCenter struct {
	ZPERNRID   string //`gorm:"primary_key"`
	KOSTL      string
	LTEXT      string
	KOSTLBUKRS string
	LTEXTBUTXT string
	BUKRS      string
	BUTXT      string
	AEDTM      string
	ABRUD      string
}

var (
	connString = fmt.Sprintf(dbString, server, port, database, user, password)
)

func main() {
	d := time.Now().Format(timeLayoutStr)
	//d = "2021-06-25"
	d = "1900-01-01"
	text := Soap12(soap12UrlPrd, d)

	doc := etree.NewDocument()
	if err := doc.ReadFromString(text); err != nil {
		panic(err)
	}

	root := doc.SelectElement("env:Envelope")

	count := 2
	var upsertCount, deleteCount int

	//dsn := fmt.Sprintf(dbString, server, port, database, user, password)
	//dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", user, password, server, port, database)
	//orm, _ := gorm.Open(sqlserver.Open(dsn), &gorm.Config{})
	//orm, _ := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	//orm.AutoMigrate(CostCenter2{})

	timeNow := time.Now().Format("2006-01-02")

	for {
		cc := new(CostCenter)
		//cc, ccFind := new(CostCenter2), new(CostCenter2)
		elements := fmt.Sprintf("./env:Body/n0:ZHR_GET_KOSTL_ZLResponse/T_KOSTL/item[%s]/*", strconv.Itoa(count))
		res := root.FindElements(elements)

		if len(res) == 0 {
			break
		}

		for _, r := range res {
			switch r.Tag {
			case "ZPERNRID":
				cc.ZPERNRID = r.Text()
			case "KOSTL":
				kostl := r.Text()
				if strings.HasPrefix(kostl, "00") {
					kostl = kostl[2:]
				}
				cc.KOSTL = kostl
			case "LTEXT":
				cc.LTEXT = r.Text()
			case "KOSTLBUKRS":
				cc.KOSTLBUKRS = r.Text()
			case "LTEXTBUTXT":
				cc.LTEXTBUTXT = r.Text()
			case "BUKRS":
				cc.BUKRS = r.Text()
			case "BUTXT":
				cc.BUTXT = r.Text()
			case "AEDTM":
				//t, _ := time.Parse(timeLayoutStr, r.Text())
				cc.AEDTM = r.Text()
			case "ABRUD":
				//t, _ := time.Parse(timeLayoutStr, r.Text())
				cc.ABRUD = r.Text()
			}
		}

		upsertQuery := fmt.Sprintf(`IF EXISTS(SELECT zpernr_id FROM cost_centers WHERE zpernr_id = '%s') UPDATE cost_centers SET [kostl]='%s', [ltext]='%s', [kostlbukrs]='%s', [ltextbutxt]='%s', [bukrs]='%s', [aedtm]='%s', [abrud]='%s', [updated_at]='%s', [deleted_at]='NULL' WHERE [zpernr_id] ='%s' ELSE INSERT INTO cost_centers (zpernr_id,kostl,ltext,kostlbukrs,ltextbutxt,bukrs,butxt,aedtm,abrud,created_at,updated_at) VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');`, cc.ZPERNRID, cc.KOSTL, cc.LTEXT, cc.KOSTLBUKRS, cc.LTEXTBUTXT, cc.BUKRS, cc.AEDTM, cc.AEDTM, timeNow, cc.ZPERNRID, cc.ZPERNRID, cc.KOSTL, cc.LTEXT, cc.KOSTLBUKRS, cc.LTEXTBUTXT, cc.BUKRS, cc.BUTXT, cc.AEDTM, cc.ABRUD, timeNow, timeNow)
		//fmt.Println(upsertQuery)
		QueryMsSql(upsertQuery)

		upsertCount++
		//fmt.Println(upsertCount)

		//orm.Find(&ccFind, "zpernr_id = ?", cc.ZPERNRID)
		//if ccFind.ZPERNRID == "" {
		//	if cc.ZPERNRID != "" {
		//		orm.Create(cc)
		//		//fmt.Println("新建" + cc.ZPERNRID)
		//		create++
		//	}
		//} else {
		//	orm.Save(cc)
		//	update++
		//	//fmt.Println("更新" + cc.ZPERNRID)
		//}
		//
		count++
	}

	// delete old ones
	deleteQuery := fmt.Sprintf(`UPDATE cost_centers SET [deleted_at]='%s' WHERE updated_at < DATEADD(DAY,-3,GETDATE())`, timeNow)
	deleteCount = QueryMsSqlRow(`SELECT COUNT(*) FROM cost_centers WHERE updated_at < DATEADD(DAY,-3,GETDATE())`)
	QueryMsSql(deleteQuery)
	//oldcc := []CostCenter2{}
	//threeDays := time.Now().AddDate(0, 0, -3)
	//orm.Where("updated_at < ?", threeDays).Find(&oldcc)
	//
	//for _, cc := range oldcc {
	//	orm.Delete(&cc)
	//}

	body := fmt.Sprintf("Create/Update: %d\nDeleted: %d", upsertCount, deleteCount)
	SendMail(MailReport, "SOAP Sync Result "+time.Now().Format("20060102"), body, "plain", "", "", MailAdmin, MailKeming)
}

func SendMail(from, subject, body, contentType, attach, bcc string, to ...string) {
	m := gomail.NewMessage()
	m.SetHeader("From", from)
	m.SetHeader("To", to...)
	if bcc != "" {
		m.SetHeader("Bcc", bcc)
	}
	//m.SetHeader("Cc", cc)
	m.SetHeader("Subject", subject)
	m.SetBody("text/"+contentType, body)
	if attach != "" {
		m.Attach(attach)
	}

	d := gomail.NewDialer(smtpServer, smtpPort, "", "")

	if err := d.DialAndSend(m); err != nil {
		fmt.Println("Sending mail failed.")
	}
}

// soap1.2
func Soap12(url, date string) string {
	reqBody := fmt.Sprintf(soapReqBody, date)

	client := &http.Client{}

	req, err := http.NewRequest("POST", url, strings.NewReader(reqBody))
	req.Header.Add("content-type", "application/soap+xml; charset=utf-8")
	req.SetBasicAuth(soapUsername, soapPassword)

	res, err := client.Do(req)
	if nil != err {
		fmt.Println("http post err:", err)
		return ""
	}
	defer res.Body.Close()

	// return status
	if http.StatusOK != res.StatusCode {
		fmt.Println("WebService soap1.2 request fail, status: %s\n", res.StatusCode)
		return ""
	}

	data, err := ioutil.ReadAll(res.Body)
	if nil != err {
		fmt.Println("ioutil ReadAll err:", err)
		return ""
	}

	return string(data)
}

func QueryMsSql(sqlquery string) *sql.Rows {
	conn, _ := sql.Open("mssql", connString)
	defer conn.Close()

	rows, _ := conn.Query(sqlquery)

	return rows
}

func QueryMsSqlRow(sqlquery string) (count int) {
	conn, _ := sql.Open("mssql", connString)
	defer conn.Close()

	conn.QueryRow(sqlquery).Scan(&count)
	//output.GenerateLog(err, "DB query", sqlquery, false)

	return count
}
