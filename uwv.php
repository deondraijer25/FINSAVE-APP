<?php

require_once(dirname(__FILE__).'/aqopi2finsave.php');
require_once(dirname(__FILE__).'/api.php');
require_once(dirname(__FILE__).'/aqopidata.php');

class Uwv extends Api {

    private $userHash;
    private $isPartner;
    
    private function __construct($connection, $guid, $userHash, $isPartner) {
        parent::__construct($connection, $guid);
        $this->userHash = $userHash;
        $this->isPartner = $isPartner;
    }
        
    private function addWerkgever($batchId, $werkgever) {
        $result = new stdClass();        
        $result->werkgever = AqopiData::addRecordItem(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_uwv_werkgever', 
            $werkgever
        );  
        while(mysqli_next_result($this->connection));
        
        if (    (!is_null($result->werkgever->data)) 
             && ($result->werkgever->data->errorcode == 0) 
             && (!is_null($result->werkgever->data->aqopi))
        ) {
            $werkgeverId = $result->werkgever->data->aqopi;
            $result->werkgever->slips = AqopiData::addChildRecordList(
                $this->connection, 
                $this->userHash,
                $batchId, 
                't_uwv_werkgever_slip', 
                $werkgever['slips'],
                'werkgever_rid',
                $werkgeverId
            ); 
            while(mysqli_next_result($this->connection));
        }
        return $result;
    }
    
    private function addWerkgevers($batchId, $uwvData) {
        $result = array();
        $data = $uwvData['werkgevers'];
        if (is_array($data)) {
            foreach ($data as $key => $werkgever) {
                $result[$key] = $this->addWerkgever($batchId, $werkgever);
            }
        }
        return $result;
    }
               
    private function addAlgemeen($batchId, $uwvData) {
        $result = AqopiData::addRecordItem(
            $this->connection, 
            $this->userHash,
            $batchId, 
            't_uwv_algemeen', 
            $uwvData
        );  
        while(mysqli_next_result($this->connection));
        return $result;
    }
  
    private function pushData($uwvData) {
        $result = new stdClass();
        $result->batchId = AqopiData::newUwvHeader(
            $this->connection, 
            $this->userHash, 
            $this->guid, 
            0, 
            $this->isPartner
        );        
        while(mysqli_next_result($this->connection));
        $batchId = $result->batchId->data;
        $this->logJson($batchId, $uwvData);
        
        $result->algemeen = $this->addAlgemeen($batchId, $uwvData);        
        $result->werkgevers = $this->addWerkgevers($batchId, $uwvData);  
        return $result;
    }
        
    private function validateData($data) {
        if (is_null($data)) {
            throw new Exception('Exception in Uwv.validateData: data is null');
        }
        if (is_null($data['uwvVerzekeringsbericht'])) {
            throw new Exception('Exception in Uwv.validateData: data.uwvVerzekeringsbericht is null');            
        }
    }
    
    private function pushResult($data) {
        $result = new stdClass();
        try { 
            $this->validateGuid(); // may raise exception
            $this->validateData($data); // may raise exceptions
            $result->data = $this->pushData($data['uwvVerzekeringsbericht']);
            $result->errorcode = '000';
        } catch (Exception $e) {
            $result->data = new stdClass();
            $result->errorcode = $this->getExceptionErrorCode($e->getMessage());
            $result->message = $this->getExceptionMessage($e->getMessage());
        } finally {
            return $result;
        }
    }
               
    static public function push($connection, $guid, $userHash, $isPartner, $data) {
        $uwv = new Uwv($connection, $guid, $userHash, $isPartner);
        $result = $uwv->pushResult($data);
        return $result;         
    }   
           
}