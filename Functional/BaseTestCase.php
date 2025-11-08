<?php

namespace Tests\Functional;

use Slim\App;
use Slim\Http\Request;
use Slim\Http\Response;
use Slim\Http\Environment;
use PHPUnit\Framework\TestCase;
use Kreait\Firebase\Factory;
use Kreait\Firebase\ServiceAccount;
use Firebase\Auth\Token\Exception\InvalidToken;
use Symfony\Component\Cache\Simple\FilesystemCache;

/**
 * This is an example class that shows how you could set up a method that
 * runs the application. Note that it doesn't cover all use-cases and is
 * tuned to the specifics of this skeleton app, so if your needs are
 * different, you'll need to change it.
 */
class BaseTestCase extends TestCase
{
    /**
     * Use middleware when running application?
     *
     * @var bool
     */
    protected $withMiddleware = true;

    /**
     * Process the application given a request method and URI
     *
     * @param string $requestMethod the request method (e.g. GET, POST, etc.)
     * @param string $requestUri the request URI
     * @param array|object|null $requestData the request data
     * @return \Slim\Http\Response
     */
    public function runApp($requestMethod, $requestUri, $requestData = null)
    {
        $idToken = $this -> createJwtToken();
        // Create a mock environment for testing with
        $environment = Environment::mock(
            [
                'REQUEST_METHOD' => $requestMethod,
                'REQUEST_URI' => $requestUri,
                'HTTP_FS-API-KEY' => $idToken
            ]
        );

        // Set up a request object based on the environment
        $request = Request::createFromEnvironment($environment);
        // Add request data, if it exists
        if (isset($requestData)) {
            $request = $request->withParsedBody($requestData);
        }

        // Set up a response object
        $response = new Response();

        // Use the application settings
        $settings = require __DIR__ . '/../../src/settings.php';

        // Instantiate the application
        $app = new App($settings);

        // Set up dependencies
        $dependencies = require __DIR__ . '/../../src/dependencies.php';
        $dependencies($app);

        // Register middleware
        if ($this->withMiddleware) {
            $middleware = require __DIR__ . '/../../src/middleware/middleware.php';
            $middleware($app);
        }

        // Register routes
        $routes = require __DIR__ . '/../../src/routes.php';
        $routes($app);

        // Process the application
        $response = $app->process($request, $response);

        // Return the response
        return $response;
    }

    private function createJwtToken() {
        $serviceAccount = ServiceAccount::fromJsonFile(__DIR__ . '/../../secrets/finsave-auth-firebase.json');
        $cache = new FilesystemCache();
        $firebase = (new Factory)
           ->withServiceAccount($serviceAccount)
           ->withVerifierCache($cache)
           ->create();
        $uid = 'mE3Bj74jmoWAhAh0CaYrryDkowB2'; //eurofacefs@gmail.com user
        $auth = $firebase->getAuth();
        $customToken = $auth->createCustomToken($uid); 
        $idTokenResponse = $auth->getApiClient()->exchangeCustomTokenForIdAndRefreshToken($customToken);
        $idToken = json_decode($idTokenResponse->getBody()->__toString(), true)['idToken'];
        return $idToken;
    }
}
