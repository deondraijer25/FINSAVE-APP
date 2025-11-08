<?php

require __DIR__.'/../../vendor/autoload.php';
use Kreait\Firebase\Factory;
use Kreait\Firebase\ServiceAccount;
use Firebase\Auth\Token\Exception\InvalidToken;
use Symfony\Component\Cache\Simple\FilesystemCache;

class FirebaseMiddleware
{
    protected $firebase;

    public function __construct()
    {
        $cache = new FilesystemCache();
        $serviceAccount = ServiceAccount::fromJsonFile(__DIR__ . '/../../secrets/finsave-auth-firebase.json');
        $this->$firebase = (new Factory)
           ->withServiceAccount($serviceAccount)
           ->withVerifierCache($cache)
           ->create();
    }

    public function __invoke($request, $response, $next)
    {
        $newRequest = $request;
        $err = [];
        if ($request->hasHeader('FS-API-KEY')) {
            $apiKey = $request->getHeaderLine('FS-API-KEY');

            try {
                $verifiedIdToken = $this->$firebase->getAuth()->verifyIdToken($apiKey);
            } catch (InvalidToken | RuntimeException $e) {
                $err = array('error' => $e->getMessage());
            }

            if (empty($err)) {
                $fUid = $verifiedIdToken->getClaim('sub');
                $newRequest = $request->withAddedHeader('fb_uid', $fUid); //pass firebase user id to API call
                $fbUser = $this->$firebase->getAuth()->getUser($fUid);
                if ($fbUser) {
                    $fbUserEmail = $fbUser->email;
                }
                if ($fbUserEmail) {
                    $newRequest = $newRequest->withAddedHeader('fb_email', $fbUserEmail);
                } else {
                    $newRequest = $newRequest->withAddedHeader('fb_email', 'unknown'); 
                }
            }
        } else {
            $err = array('error' => 'FS-API-KEY is missing in headers');
        }
        if (!empty($err)) {
            return $response->withJson($err, 401);
        }
        $response = $next($newRequest, $response); 
        return $response;
    }
}