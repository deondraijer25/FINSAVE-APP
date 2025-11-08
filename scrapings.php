<?php

require_once(dirname(__FILE__).'/api.php');
require_once(dirname(__FILE__).'/iwizedata.php');

class Scrapings extends Api {

    private $metaList;
    private $firebase_uid;
    private $firebase_email;
    private $isPartner;
    
    private function __construct($connection, $guid, $firebase_uid, $firebase_email, $isPartner) {
        parent::__construct($connection, $guid);
        $this->firebase_uid = $firebase_uid;
        $this->firebase_email = $firebase_email;
        $this->isPartner = $isPartner;
        $this->metaList = array();
    }

    private function addMeta($meta, $tableName, $columnName = 'ALL_COLUMNS') {
        $item = array();
        $item['table_name'] = $tableName;
        $item['column_name'] = $columnName;
        $item['meta_source'] = $meta['bron'];
        $item['meta_date'] = $this->mySqlDateString($meta['datum']);
        $this->metaList[] = $item;
    }

    protected function logJson($batchId, $data) {
        $data = json_encode($data);
        $sql = "
            insert
              into iwize.t_batch_json
                 ( batch_rid
                 , json
                 )
            select ?
                 , ?
        ";         
        $statement = $this->connection->prepare($sql);  
        if (!$statement) {
            throw new Exception('SQL Exception in '. __CLASS__ . '->' . __FUNCTION__ . ': ' . $this->connection->error);
        }
        $statement->bind_param(
          'is'
        , $batchId
        , $data
        );        
        if (!$statement->execute()) {
            throw new Exception('Exception in '. __CLASS__ . '->' . __FUNCTION__ . ': ' . $statement->error);
        }; 
        $statement->close();    
    }  

    private function extractOnlySingleVal($data, $metaTableName) {
        /*
        // this is the json represention, in php these are associative arrays:
        //
        // input { 
        //   "column1": {
        //     "val": "value1",
        //     "meta": {
        //       "bron": "moh",
        //       "datum": "21-12-2019"
        //     }
        //   },
        //   "column2": {
        //     "val": "value2",
        //     "meta": {
        //       "bron": "moh",
        //       "datum": "21-12-2019"
        //     }
        //   }
        // }
        //
        // output {
        //   "column1": "value1",
        //   "column2": "value2"
        // }
        */
        $result = array();        
        foreach ($data as $columnName => $columnValue) {
            if (($columnValue) && ($val = $columnValue["val"])) {
                if (!is_array($val)) {
                    $result[$columnName] = $val;
                    $this->addMeta($columnValue["meta"], $metaTableName, $columnName);
                }
            }
        }
        return $result;
    }

    private function convertValuesToColumnValues($data, $columnName) {
        /*
        // this is the json represention, in php these are associative arrays:
        //
        // input { 
        //   [ 
        //     "value1",
        //     "value2"
        //   ]
        // }
        //
        // output {
        //   {"columnName": "value1"},
        //   {"columnName": "value2"}
        // }
        */
        $result = array();  
        foreach ($data as $columnValue) {
            if ($columnValue) {
                $item = array();
                $item[$columnName] = $columnValue;
                $result[] = $item;
            }
        }
        return $result;
    }

    private function addPgBankrekeningnummers($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_bankrekeningnummer';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $recordList = $this->convertValuesToColumnValues($val, 'waarde');
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName, 
            $recordList
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addPgBurgerlijkeStaat($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_burgerlijkeStaat';
        $recordItem = $this->extractOnlySingleVal($data, $tableName);
        $result = IwizeData::addRecordItem(
            $this->connection, 
            $batchId, 
            $tableName, 
            $recordItem
        ); 
        return $result;
    }
    
    private function addPgAanvrager($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_aanvrager';
        $recordItem = $this->extractOnlySingleVal($data, $tableName);
        $result->aanvrager = IwizeData::addRecordItem(
            $this->connection, 
            $batchId, 
            $tableName, 
            $recordItem
        ); 
        $result->bankrekeningnummers = $this->addPgBankrekeningnummers($batchId, $data["bankrekeningnummers"]);
        $result->burgerlijkeStaat = $this->addPgBurgerlijkeStaat($batchId, $data["burgerlijkeStaat"]);
        return $result;
    }

    private function addPgAdressen($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_adres';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName, 
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addPgAdressenOnroerendGoedGecombineerd($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $val = $data['val'];
        $tableName = 't_adresOnroerendGoedGecombineerd';
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        /*
        // $val is a list (normally use IwizeData::addRecordList) but items have a child, 
        // so loop through list and use IwizeData::addRecordItem
        // and then save child with IwizeData.addChildRecordList.
        */
        $items = array();
        foreach ($val as $recordItem) {
            $item = IwizeData::addRecordItem(
                $this->connection, 
                $batchId,
                $tableName,
                $recordItem
            ); 
            if (    ($item) 
                 && ($item->errorcode == '000') 
                 && ($item->data) 
                 && ($item->data->iwize) 
                 && ($recordItem['wozWaardeOntwikkeling'])
            ) {
                $item->wozWaardeOntwikkeling = IwizeData::addChildRecordList(
                    $this->connection, 
                    $batchId, 
                    't_aogg_wozWaardeOntwikkeling', 
                    $recordItem['wozWaardeOntwikkeling'],
                    'adresOnroerendGoedGecombineerd_rid',
                    $item->data->iwize
                );
            }
            $items[] = $item;
        }
        $result = $items;
        return $result; 
    }

    private function addPgIdentificatiedocumenten($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_identificatiedocument';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addPgPartner($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_partner';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordItem(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addPgVoormaligePartners($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_voormaligePartner';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addPgKinderen($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_kind';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addPgOuders($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_ouder';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addPersoonsgegevens($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $result->aanvrager = $this->addPgAanvrager($batchId, $data['aanvrager']);
        $result->adressen = $this->addPgAdressen($batchId, $data['adressen']);
        $result->adressenOnroerendGoedGecombineerd = $this->addPgAdressenOnroerendGoedGecombineerd($batchId, $data['adressenOnroerendGoedGecombineerd']);
        $result->identificatiedocumenten = $this->addPgIdentificatiedocumenten($batchId, $data['identificatiedocumenten']);
        $result->partner = $this->addPgPartner($batchId, $data['partner']);
        $result->voormaligePartners = $this->addPgVoormaligePartners($batchId, $data['voormaligePartners']);
        $result->kinderen = $this->addPgKinderen($batchId, $data['kinderen']);
        $result->ouders = $this->addPgOuders($batchId, $data['ouders']);
        return $result;
    }

    private function addIkToetsinkomen($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_toetsinkomen';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordItem(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addIkVerzamelinkomens($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_verzamelinkomen';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addIkArbeidsverledenDetails($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_arbeidsverledenDetail';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        /*
        // $val is a list (normally use IwizeData::addRecordList) but items have a child, 
        // so loop through list and use IwizeData::addRecordItem
        // and then save child with IwizeData.addChildRecordList.
        */
        $items = array();
        foreach ($val as $recordItem) {
            $item = IwizeData::addRecordItem(
                $this->connection, 
                $batchId, 
                $tableName,
                $recordItem
            ); 
            if (    ($item) 
                 && ($item->errorcode == '000') 
                 && ($item->data) 
                 && ($item->data->iwize) 
                 && ($recordItem['werkgevers'])
            ) {
                $item->werkgevers = IwizeData::addChildRecordList(
                    $this->connection, 
                    $batchId, 
                    't_avd_werkgever', 
                    $recordItem['werkgevers'],
                    'arbeidsverledenDetail_rid',
                    $item->data->iwize
                );
            }
            $items[] = $item;
        }
        $result = $items;
        return $result;
    }

    private function addIkArbeidsverleden($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_arbeidsverleden';
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $recordItem = $this->extractOnlySingleVal($data, $tableName);
        $result->arbeidsverleden = IwizeData::addRecordItem(
            $this->connection, 
            $batchId, 
            $tableName, 
            $recordItem
        ); 
        $result->arbeidsverledenDetails = $this->addIkArbeidsverledenDetails($batchId, $data["arbeidsverledenDetails"]);
        return $result;
    }

    private function addIkWerkgevers($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_werkgever';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        /*
        // $val is a list (normally use IwizeData::addRecordList) but items have a child, 
        // so loop through list and use IwizeData::addRecordItem
        // and then save child with IwizeData.addChildRecordList.
        */
        $items = array();
        foreach ($val as $recordItem) {
            $item = IwizeData::addRecordItem(
                $this->connection, 
                $batchId, 
                $tableName,
                $recordItem
            ); 
            if (    ($item) 
                 && ($item->errorcode == '000') 
                 && ($item->data) 
                 && ($item->data->iwize) 
                 && ($recordItem['slips'])
            ) {
                $item->slips = IwizeData::addChildRecordList(
                    $this->connection, 
                    $batchId, 
                    't_werkgever_slip', 
                    $recordItem['slips'],
                    'werkgever_rid',
                    $item->data->iwize
                );
            }
            $items[] = $item;
        }
        $result = $items;
        return $result;
    }

    private function addIkBelastingaangifte($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_belastingaangifte';
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $recordItem = $this->extractOnlySingleVal($data, $tableName);
        $result = IwizeData::addRecordItem(
            $this->connection, 
            $batchId, 
            $tableName, 
            $recordItem
        ); 
        if (    ($result) 
             && ($result->errorcode == '000') 
             && ($result->data) 
             && ($result->data->iwize) 
             && ($data['inkomens'])
        ) {
            $result->inkomens = IwizeData::addChildRecordList(
                $this->connection, 
                $batchId, 
                't_belastingaangifte_inkomen', 
                $data['inkomens'],
                'belastingaangifte_rid',
                $result->data->iwize
            );
        }
        return $result;
    }

    private function addIkOntvangenAlimentatie($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $result = IwizeData::addRecordItem(
            $this->connection, 
            $batchId, 
            't_ontvangenAlimentatie', 
            $data
        );
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addInkomens($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $result->toetsinkomen = $this->addIkToetsinkomen($batchId, $data['toetsinkomen']);
        $result->verzamelinkomens = $this->addIkVerzamelinkomens($batchId, $data['verzamelinkomens']);
        $result->arbeidsverleden = $this->addIkArbeidsverleden($batchId, $data['arbeidsverleden']);
        $result->werkgevers = $this->addIkWerkgevers($batchId, $data['werkgevers']);
        $result->belastingaangifte = $this->addIkBelastingaangifte($batchId, $data['belastingaangifte']);
        $result->ontvangenAlimentatie = $this->addIkOntvangenAlimentatie($batchId, $data['ontvangenAlimentatie']);
        return $result;
    }

    private function addBzVoertuigen($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_voertuig';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addBzOnroerendGoed($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_onroerendGoed';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        /*
        // $val is a list (normally use IwizeData::addRecordList) but items have a child, 
        // so loop through list and use IwizeData::addRecordItem
        // and then save child with IwizeData.addChildRecordList.
        */
        $items = array();
        foreach ($val as $recordItem) {
            $item = IwizeData::addRecordItem(
                $this->connection, 
                $batchId, 
                $tableName,
                $recordItem
            ); 
            if (    ($item) 
                 && ($item->errorcode == '000') 
                 && ($item->data) 
                 && ($item->data->iwize) 
                 && ($recordItem['wozWaardeOntwikkeling'])
            ) {
                $item->wozWaardeOntwikkeling = IwizeData::addChildRecordList(
                    $this->connection, 
                    $batchId, 
                    't_og_wozWaardeOntwikkeling', 
                    $recordItem['wozWaardeOntwikkeling'],
                    'onroerendGoed_rid',
                    $item->data->iwize
                );
            }
            $items[] = $item;
        }
        $result = $items;
        return $result; 
    }

    private function addBzBankrekeningen($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_bankrekening';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addBzKapitaalverzekeringen($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_kapitaalverzekering';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addBzPremiedepots($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_premiedepots';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addBezittingen($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $result->voertuigen = $this->addBzVoertuigen($batchId, $data['voertuigen']);
        $result->onroerendGoed = $this->addBzOnroerendGoed($batchId, $data['onroerendGoed']);
        $result->bankrekeningen = $this->addBzBankrekeningen($batchId, $data['bankrekeningen']);
        $result->kapitaalverzekeringen = $this->addBzKapitaalverzekeringen($batchId, $data['kapitaalverzekeringen']);
        $result->premiedepots = $this->addBzPremiedepots($batchId, $data['premiedepots']);
        return $result;
    }

    private function addBpLijfrente($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_lijfrente';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addBpInlegbetalingen($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_inlegbetaling';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addBpNettoPensioen($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_nettoPensioen';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addBpAov($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_aov';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addBetaaldePremies($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $result->lijfrente = $this->addBpLijfrente($batchId, $data['lijfrente']);
        $result->inlegbetalingen = $this->addBpInlegbetalingen($batchId, $data['inlegbetalingen']);
        $result->nettoPensioen = $this->addBpNettoPensioen($batchId, $data['nettoPensioen']);
        $result->aov = $this->addBpAov($batchId, $data['aov']);
        return $result;
    }

    private function addHypotheekdelen($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_hypotheekdeel';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }    

    private function addStudieschuld($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_studieschuld';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordItem(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }    

    private function addOverigeSchulden($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_overigeSchuld';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        if ($val->saldo) {
            $val['saldoBeginstand'] = $val->saldo->beginstand;
            $val['saldoEindstand'] = $val->saldo->eindstand;
        }
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }    

    private function addSchulden($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $result->hypotheekdelen = $this->addHypotheekdelen($batchId, $data['hypotheekdelen']);
        $result->studieschuld = $this->addStudieschuld($batchId, $data['studieschuld']);
        $result->overigeSchulden = $this->addOverigeSchulden($batchId, $data['overigeSchulden']);
        return $result;
    }

    private function addPnAlgemeen($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_pensioen_algemeen';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordItem(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addPnPensioenTotalen($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_pensioenTotaal';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addPnOuderdomPensioenDetails($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_ouderdomPensioenDetail';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        /*
        // $val is a list (normally use IwizeData::addRecordList) but items have a child, 
        // so loop through list and use IwizeData::addRecordItem
        // and then save child with IwizeData.addChildRecordList.
        */
        $items = array();
        foreach ($val as $recordItem) {
            $item = new stdClass();
            $item->val = IwizeData::addRecordItem(
                $this->connection, 
                $batchId, 
                $tableName,
                $recordItem
            ); 
            if (    ($item->val) 
                 && ($item->val->errorcode == '000') 
                 && ($item->val->data) 
                 && ($item->val->data->iwize)
            ) {
                if ($recordItem['pensioenen']) {
                    $item->pensioenen = IwizeData::addChildRecordList(
                        $this->connection, 
                        $batchId, 
                        't_opd_pensioen', 
                        $recordItem['pensioenen'],
                        'ouderdomPensioenDetail_rid',
                        $item->val->data->iwize
                    );
                }
                if ($recordItem['pensioenenIndicatief']) {
                    $item->pensioenenIndicatief = IwizeData::addChildRecordList(
                        $this->connection, 
                        $batchId, 
                        't_opd_pensioenIndicatief', 
                        $recordItem['pensioenenIndicatief'],
                        'ouderdomPensioenDetail_rid',
                        $item->val->data->iwize
                    );
                }
                if ($recordItem['pensioenUitkeringen']) {
                    $item->pensioenUitkeringen = IwizeData::addChildRecordList(
                        $this->connection, 
                        $batchId, 
                        't_opd_pensioenUitkering', 
                        $recordItem['pensioenUitkeringen'],
                        'ouderdomPensioenDetail_rid',
                        $item->val->data->iwize
                    );
                }
                if ($recordItem['aow']) {
                    $item->aow = IwizeData::addChildRecordItem(
                        $this->connection, 
                        $batchId, 
                        't_opd_aow', 
                        $recordItem['aow'],
                        'ouderdomPensioenDetail_rid',
                        $item->val->data->iwize
                    );
                }
            }
            $items[] = $item;
        }
        $result = $items;
        return $result; 
    }

    private function addPnPartnerPensioenDetails($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_partnerPensioenDetail';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        /*
        // $val is a list (normally use IwizeData::addRecordList) but items have a child, 
        // so loop through list and use IwizeData::addRecordItem
        // and then save child with IwizeData.addChildRecordList.
        */
        $items = array();
        foreach ($val as $recordItem) {
            $item = new stdClass();
            $item->val = IwizeData::addRecordItem(
                $this->connection, 
                $batchId, 
                $tableName,
                $recordItem
            ); 
            if (    ($item->val) 
                 && ($item->val->errorcode == '000') 
                 && ($item->val->data) 
                 && ($item->val->data->iwize)
            ) {
                if ($recordItem['pensioenen']) {
                    $item->pensioenen = IwizeData::addChildRecordList(
                        $this->connection, 
                        $batchId, 
                        't_ppd_pensioen', 
                        $recordItem['pensioenen'],
                        'partnerPensioenDetail_rid',
                        $item->val->data->iwize
                    );
                }
                if ($recordItem['pensioenenIndicatief']) {
                    $item->pensioenenIndicatief = IwizeData::addChildRecordList(
                        $this->connection, 
                        $batchId, 
                        't_ppd_pensioenIndicatief', 
                        $recordItem['pensioenenIndicatief'],
                        'partnerPensioenDetail_rid',
                        $item->val->data->iwize
                    );
                }
            }
            $items[] = $item;
        }
        $result = $items;
        return $result; 
    }

    private function addPnWezenPensioenDetails($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_wezenPensioenDetail';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        /*
        // $val is a list (normally use IwizeData::addRecordList) but items have a child, 
        // so loop through list and use IwizeData::addRecordItem
        // and then save child with IwizeData.addChildRecordList.
        */
        $items = array();
        foreach ($val as $recordItem) {
            $item = new stdClass();
            $item->val = IwizeData::addRecordItem(
                $this->connection, 
                $batchId, 
                $tableName,
                $recordItem
            ); 
            if (    ($item->val) 
                 && ($item->val->errorcode == '000') 
                 && ($item->val->data) 
                 && ($item->val->data->iwize)
            ) {
                if ($recordItem['pensioenen']) {
                    $item->pensioenen = IwizeData::addChildRecordList(
                        $this->connection, 
                        $batchId, 
                        't_wpd_pensioen', 
                        $recordItem['pensioenen'],
                        'wezenPensioenDetail_rid',
                        $item->val->data->iwize
                    );
                }
                if ($recordItem['pensioenenIndicatief']) {
                    $item->pensioenenIndicatief = IwizeData::addChildRecordList(
                        $this->connection, 
                        $batchId, 
                        't_wpd_pensioenIndicatief', 
                        $recordItem['pensioenenIndicatief'],
                        'wezenPensioenDetail_rid',
                        $item->val->data->iwize
                    );
                }
            }
            $items[] = $item;
        }
        $result = $items;
        return $result; 
    }

    private function addPnPensioenDetails($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $result->ouderdomPensioenDetails = $this->addPnOuderdomPensioenDetails($batchId, $data['ouderdomPensioenDetails']);
        $result->partnerPensioenDetails = $this->addPnPartnerPensioenDetails($batchId, $data['partnerPensioenDetails']);
        $result->wezenPensioenDetails = $this->addPnWezenPensioenDetails($batchId, $data['wezenPensioenDetails']);
        return $result;
    }

    private function addPnPartnerPensioenTotalen($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_partnerPensioenTotaal';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addPnWezenPensioenTotalen($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $tableName = 't_wezenPensioenTotaal';
        $val = $data['val'];
        if (!$val) {
            $result->message = 'No val found in data';
            return $result;
        }
        $this->addMeta($data['meta'], $tableName); 
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function addPensioenen($batchId, $data) {
        $result = new stdClass();
        if (!$data) {
            $result->message = 'No data found';
            return $result;
        }
        $result->algemeen = $this->addPnAlgemeen($batchId, $data['algemeen']);
        $result->pensioenTotalen = $this->addPnPensioenTotalen($batchId, $data['pensioenTotalen']);
        $result->pensioenDetails = $this->addPnPensioenDetails($batchId, $data['pensioenDetails']);
        $result->partnerPensioenTotalen = $this->addPnPartnerPensioenTotalen($batchId, $data['partnerPensioenTotalen']);
        $result->wezenPensioenTotalen = $this->addPnWezenPensioenTotalen($batchId, $data['wezenPensioenTotalen']);
        return $result;
    }

    private function newBatchId($uuid) {
        $response = IwizeData::newBatch(
            $this->connection, 
            $this->guid,
            $this->firebase_uid, 
            $this->firebase_email,
            $uuid, 
            $this->isPartner
        );        
        while(mysqli_next_result($this->connection));
        if ($response->errorcode != '000') {
            throw new Exception('Exception in '. __CLASS__ . '->' . __FUNCTION__ . ': ' . $response->message);
        }
        return $response->data;
    }

    private function mySqlDateString(string $dateString) {
        $date = DateTime::createFromFormat('d-m-Y', $dateString);
        if (!$date) {
            throw new Exception('Exception in '. __CLASS__ . '->' . __FUNCTION__ . ": can not create datetime (dd-mm-yyyy) from string literal [$dateString]");
        }
        return $date->format('Y-m-d');
    }

    private function addMetaList($batchId) {
        $result = new stdClass();
        $tableName = 't_batch_meta';
        $val = $this->metaList;
        if (!$val) {
            $result->message = 'No metaList found';
            return $result;
        }
        $result = IwizeData::addRecordList(
            $this->connection, 
            $batchId, 
            $tableName,
            $val
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }

    private function callStoredProd($storedProc, $guid, $isPartner) {
        $query = $this->connection->query("call iwize.$storedProc('$guid', $isPartner)"); 
        if (!$query) { 
            throw new Exception("SQL Exception in $storedProc: " . $this->connection->error);
        }   
        $query->fetch_all(MYSQLI_ASSOC); 
        while ($this->connection->next_result()) {};
        return "Called iwize.$storedProc('$guid', $isPartner)"; 
    }

  
    private function pushData($data) {
        $result = new stdClass();
        $uuid = $data['uuid'];
        $batchId = $this->newBatchId($uuid);
        $this->logJson($batchId, $data);        
        $result->batchId = $batchId;
        $result->persoonsgegevens = $this->addPersoonsgegevens($batchId, $data['persoonsgegevens']);        
        $result->inkomens = $this->addInkomens($batchId, $data['inkomens']);       
        $result->bezittingen = $this->addBezittingen($batchId, $data['bezittingen']);       
        $result->betaaldePremies = $this->addBetaaldePremies($batchId, $data['betaaldePremies']);       
        $result->schulden = $this->addSchulden($batchId, $data['schulden']);       
        $result->pensioenen = $this->addPensioenen($batchId, $data['pensioenen']);  
        $result->metaList = $this->addMetaList($batchId);
        return $result;
    }

    private function processSync() {
        $guid = $this->guid;
        if ($this->isPartner) {
            $isPartner = 1;
        } else {
            $isPartner = 0;
        }
        /*
        // do not call iwize.p_process_all which does not work reliable from php (for some reason
        // calling sp which calls other sp's doesnt work well)
        // in stead call all stored procedures p_process_all calls from php:
        */      
        $result[] = $this->callStoredProd('p_process_aanvrager', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_adres', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_adresOnroerendGoedGecombineerd', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_arbeidsverleden', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_bankrekening_update', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_bankrekening_delete', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_bankrekening_insert', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_burgerlijkeStaat', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_hypotheekdeel', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_identificatiedocument', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_kind', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_lijfrente_update', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_lijfrente_delete', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_lijfrente_insert', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_onroerendGoed_update', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_onroerendGoed_delete', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_onroerendGoed_insert', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_overigeSchuld_update', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_overigeSchuld_delete', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_overigeSchuld_insert', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_partner', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_pensioen_update', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_pensioen_delete', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_pensioen_insert', $guid, $isPartner);
        $result[] = $this->callStoredProd('p_process_werkgever', $guid, $isPartner);
        return $result;
    }
        
    private function validateData($data) {
        if (is_null($data)) {
            throw new Exception('Exception in '. __CLASS__ . '->' . __FUNCTION__ . ': data is null');
        }
        if (is_null($data['uuid'])) {
            throw new Exception('Exception in '. __CLASS__ . '->' . __FUNCTION__ . ': data.uuid is null');            
        }
    }
  
    private function latestData() {
        $sql = "
          select latestBatch.owner
               , latestBatch.type
               , latestBatch.date
               , case
                   when datediff(latestBatch.date, setting.refreshed_date) < 0 
                     then 'Y' 
                     else 'N'
                   end as canRefresh
            from (
              select case 
                       when b.is_partner = 0 
                         then 'Client'
                         else 'Partner'
                       end as owner
                   , upper(b_m.meta_source) as type 
                   , max(meta_date) as date
                from iwize.t_batch b 
                     join iwize.t_batch_meta b_m
                       on b_m.batch_rid = b.batch_id
               where b.finsave_guid = ?  
               group
                  by case 
                       when b.is_partner = 0 
                         then 'Client'
                         else 'Partner'
                       end
                   , upper(b_m.meta_source)
           ) as latestBatch
                join (
                  select 'MBD' as data_type
                       , date(setting_value) as refreshed_date
                    from aqopiapp.t_setting 
                   where setting_key = 'MBD_REFRESHED'
                  union
                  select 'MOH' as data_type
                       , date(setting_value) as refreshed_date
                    from aqopiapp.t_setting 
                   where setting_key = 'MOH_REFRESHED'
                  union
                  select 'MPO' as data_type
                       , date(setting_value) as refreshed_date
                    from aqopiapp.t_setting 
                   where setting_key = 'MPO_REFRESHED'
                  union
                  select 'UWV' as data_type
                       , date(setting_value) as refreshed_date
                    from aqopiapp.t_setting 
                   where setting_key = 'UWV_REFRESHED'
                ) as setting
                  on setting.data_type = latestBatch.type
           order
              by latestBatch.owner 
               , latestBatch.type
        "; 
        $statement = $this->connection->prepare($sql);       
        if  (!$statement) {
            throw new Exception('SQL Exception in '. __CLASS__ . '->' . __FUNCTION__ . ': ' . $this->connection->error);    
        }         
        $statement->bind_param(
          's'
        , $this->guid
        );            
        if (!$statement->execute()) {
            throw new Exception('Exception in '. __CLASS__ . '->' . __FUNCTION__ . ': ' . $statement->error);
        };       
        $statement->bind_result(
            $owner,
            $type,
            $date,
            $canRefresh
        );            
        $result = array();       
        while ($statement->fetch()) {
            $row["owner"] = $owner;
            $row["type"] = $type;
            $row["date"] = $date;
            $row["canRefresh"] = $canRefresh;
            $result[] = $row;
        }                      
        $statement->close();
        return $result;         
    }
       
    private function pushResult($data) {
        $result = new stdClass();
        try { 
            $this->validateGuid(); // may raise exception
            $this->validateData($data); // may raise exceptions
            $result->data = $this->pushData($data);
            $result->sync = $this->processSync();
            $result->errorcode = '000';
        } catch (Exception $e) {
            $result->data = new stdClass();
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    }

    private function latestResult() {
        $result = new stdClass();
        try {                     
            $result->data = $this->latestData();
            $result->errorcode = '000';
        } catch (Exception $e) {
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    }      
                     
    static public function push($connection, $guid, $firebase_uid, $firebase_email, $isPartner, $data) {
        $scrapings = new Scrapings($connection, $guid, $firebase_uid, $firebase_email, $isPartner);
        $result = $scrapings->pushResult($data);
        return $result;         
    } 

    static public function latest($connection, $guid) {
        $iwizeData = new Scrapings($connection, $guid, '', '', 0);
        $result = $iwizeData->latestResult();
        return $result;
    }  
           
}