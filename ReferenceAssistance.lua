luanet.load_assembly("log4net");

local types = {}
types["log4net.LogManager"] = luanet.import_type("log4net.LogManager");
local log = types["log4net.LogManager"].GetLogger("AtlasSystems.Addons.OnShelfNotifications");

local Settings = {};
Settings.NVTGC = GetSetting("NVTGC");
Settings.MonitorQueues = GetSetting("MonitorQueues");
Settings.EmailName = GetSetting("EmailName");
Settings.LibraryUseOnlyEmailName = GetSetting("LibraryUseOnlyEmailName");
Settings.NotificationTime = GetSetting("NotificationTime");
Settings.NotificationDaysOfWeek = GetSetting("NotificationDaysOfWeek"):lower();
Settings.NotificationWaitDays = GetSetting("NotificationWaitDays");
Settings.OnShelfRemovalDays = tonumber(GetSetting("OnShelfRemovalDays"));
Settings.DueDateRemovalDays = tonumber(GetSetting("DueDateRemovalDays"));
Settings.RemoveFromShelfQueue = GetSetting("RemoveFromShelfQueue");

local isCurrentlyProcessing = false;
local sharedServerSupport = false;
local lastCheckedDay = nil;
local hasRunToday = false;
local systemManagerAddonInterval = nil;

function Init()
    RegisterSystemEventHandler("SystemTimerElapsed", "TimerElapsed");
end

function TimerElapsed()
    if not isCurrentlyProcessing then
        DailyRunTimeReset();

        if hasRunToday or not IsTimeToRun() then
            return;
        end
    
        local connection = CreateManagedDatabaseConnection();

        local success, requestsOrErr = pcall(function()
            connection:Connect();
            SetSharedServerSupport(connection);
    
            local usersTable = "Users";
            if sharedServerSupport then
                usersTable = "UsersALL";
            end
    
            local queryString = [[SELECT DISTINCT Transactions.TransactionNumber, Transactions.LibraryUseOnly, Transactions.DueDate, Tracking.DateTime FROM Transactions 
            INNER JOIN Tracking ON Tracking.TransactionNumber = Transactions.TransactionNumber 
            INNER JOIN ]] .. usersTable .. [[ ON ]] .. usersTable .. [[.Username = Transactions.Username 
            WHERE Transactions.TransactionStatus = 'Customer Notified via E-Mail'
            AND Tracking.ChangedTo = 'Customer Notified via E-Mail'
            AND DATEADD(day, ]] .. Settings.NotificationWaitDays .. [[, Tracking.DateTime) <= CAST(GETDATE() AS DATE)]];
    
            if Settings.NVTGC:find("%w") then
                Settings.NVTGC = "'" .. Settings.NVTGC:gsub("%s*,%s*", ","):gsub(",", "','") .. "'";
                queryString = queryString .. " AND NVTGC IN(" .. Settings.NVTGC .. ")";
            end
    
            log:Debug("Querying the database with querystring: " .. queryString);

            connection.QueryString = queryString;
            local queryResults = connection:Execute();
    
            local requests = {};
            if queryResults.Rows.Count > 0 then
                for i = 0, queryResults.Rows.Count - 1 do
                    local index = #requests+1;
                    requests[index] = {};

                    requests[index]["TransactionNumber"] = queryResults.Rows:get_Item(i):get_Item("TransactionNumber");
                    requests[index]["LibraryUseOnly"] = queryResults.Rows:get_Item(i):get_Item("LibraryUseOnly");
                    requests[index]["DueDate"] = queryResults.Rows:get_Item(i):get_Item("DueDate");
                    requests[index]["OnShelfDate"] = queryResults.Rows:get_Item(i):get_Item("DateTime");
                end
            end
    
            return requests;
        end);
    
        connection:Dispose();
    
        if success then
            hasRunToday = true;

            if #requestsOrErr > 0 then
                ProcessRequests(requestsOrErr);
            end
        else
            log:Error("An error occurred when retrieving transaction info from the database: " .. tostring(TraverseError(requestsOrErr)));
        end

        isCurrentlyProcessing = false;
    else
        log:Debug("Still processing requests for on shelf notifications.");
    end
end

function DailyRunTimeReset()
    local today = os.date("%A"):lower();

    if lastCheckedDay ~= today then
        -- We don't want to log this on the first run of the addon where lastCheckedDay will be nil.
        if lastCheckedDay then
            log:Debug("Date has changed. hasRunToday will be set to false and cached SystemManagerAddonInterval will be updated.");
        end
        hasRunToday = false;
        lastCheckedDay = today;

        -- Update cached value for SystemManagerAddonInterval in case it has changed.
        local connection = CreateManagedDatabaseConnection();
        local success, err = pcall(function()
            connection:Connect();
            CacheSystemManagerAddonInterval(connection);
        end);

        connection:Dispose();

        if not success then
            log:Error("An error occurred when retrieving SystemManagerAddonInterval from the database: " .. tostring(TraverseError(err)));
        end

    end
    
    if hasRunToday then
        log:Debug("On Shelf Notifications has already run today and will not run again until the next designated day.");
    end
end

function IsTimeToRun()
    -- Cache SystemManagerAddonInterval if it is not cached already.
    if not systemManagerAddonInterval then
        local connection = CreateManagedDatabaseConnection();
        local success, err = pcall(function()
            connection:Connect();
            CacheSystemManagerAddonInterval(connection);

        end);

        connection:Dispose();

        if not success then
            log:Error("An error occurred when retrieving SystemManagerAddonInterval from the database: " .. tostring(TraverseError(err)));
        end
    end
    
    local currentDayOfWeek = os.date("%A"):lower();
    local currentDate = os.date("%m/%d/%Y");
    local thisMonth, today, thisYear = tostring(currentDate):match("(%d+)/(%d+)/(%d+)");
    local currentTimeSeconds = os.time();
    
    local runTimeHour, runTimeMinute = Settings.NotificationTime:match("(%d%d)(%d%d)");
    local runTimeMinSeconds = os.time({year=thisYear, month=thisMonth, day=today, hour=runTimeHour, min=runTimeMinute});
    local runTimeMaxSeconds = runTimeMinSeconds + (systemManagerAddonInterval * 60 * 3);

    -- The addon can run between the runtime and the runtime + 3 times the SystemManagerAddonInterval.
    -- This is to prevent the addon from sending notifications immediately every time it's turned on or
    -- System Manager is restarted, which would happen if using simply currentTimeSeconds >= runTimeSeconds.
    -- The range is given in terms of the SystemManagerAddonInterval to ensure runs aren't skipped.
    if currentTimeSeconds >= runTimeMinSeconds and currentTimeSeconds <= runTimeMaxSeconds and Settings.NotificationDaysOfWeek:find(currentDayOfWeek) then
        log:Debug("Run time criteria met.");
        return true;
    end
    
    -- Values logged for support when addon does not run.
    log:Debug("Criteria for run time not met. \nCurrent time: " .. os.date("%H%M", currentTimeSeconds) .. "\nMinimum run time: " .. os.date("%H%M", runTimeMinSeconds) .. "\nMaximum runtime: " .. os.date("%H%M", runTimeMaxSeconds) .. "\nCurrent day of the week: " .. currentDayOfWeek);

    return false;
end

function ProcessRequests(requests)
    -- This is to ensure dates are compared to the current date at midnight regardless of the current time.
    local currentDate = os.date("%m/%d/%Y");
    local thisMonth, today, thisYear = tostring(currentDate):match("(%d+)/(%d+)/(%d+)");
    local currentDateSeconds = os.time({year=thisYear, month=thisMonth, day=today});

    for i = 1, #requests do
        -- Routing items to the RemoveFromShelfQueue happens first so notifications don't get
        -- sent for items that are removed.
        local routed = false;
        local transactionNumber = requests[i]["TransactionNumber"];

        log:Debug("Processing transaction " .. transactionNumber);

        if type(Settings.DueDateRemovalDays) == "number" and Settings.RemoveFromShelfQueue:find("%a") then
            local dueDate = tostring(requests[i]["DueDate"]);

            if not dueDate or not dueDate:find("%d+/%d+/%d+") then
                log:Warn("Transaction " .. transactionNumber .. " does not have a valid due date. It cannot be routed to " .. Settings.RemoveFromShelfQueue .. " based on due date.");
            else
                local month, day, year = dueDate:match("(%d+)/(%d+)/(%d+)");
                local dueDateSeconds = os.time({year=year, month=month, day=day});
    
                if currentDateSeconds >= (dueDateSeconds - (Settings.DueDateRemovalDays * 24 * 60 * 60)) then
                    log:Debug("Transaction " .. transactionNumber .. " is " .. tostring(Settings.DueDateRemovalDays) .. " days or less from its due date. Routing to " .. Settings.RemoveFromShelfQueue .. ".");
                    ExecuteCommand("Route", {transactionNumber, Settings.RemoveFromShelfQueue});
                    routed = true;
                end
            end
        end

        local month, day, year = tostring(requests[i]["OnShelfDate"]):match("(%d+)/(%d+)/(%d+)");
        local onShelfDateSeconds = os.time({year=year, month=month, day=day});

        if not routed and type(Settings.OnShelfRemovalDays) == "number" and Settings.RemoveFromShelfQueue:find("%a") then
            if currentDateSeconds >= (onShelfDateSeconds + (Settings.OnShelfRemovalDays * 24 * 60 * 60)) then
                log:Debug("Transaction " .. transactionNumber .. " has been on the shelf for at least " .. tostring(Settings.OnShelfRemovalDays) .. " days. Routing to " .. Settings.RemoveFromShelfQueue .. ".");
                ExecuteCommand("Route", {transactionNumber, Settings.RemoveFromShelfQueue});
                routed = true;
            end
        end

        if not routed then
            if Settings.LibraryUseOnlyEmailName:find("%a") and requests[i]["LibraryUseOnly"] == "Yes" then
                log:Debug("Sending on shelf notification with template " .. Settings.LibraryUseOnlyEmailName .. " for transaction " .. transactionNumber .. ".");
                ExecuteCommand("SendTransactionNotification", {transactionNumber, Settings.LibraryUseOnlyEmailName});
            else
                log:Debug("Sending on shelf notification with template " .. Settings.EmailName .. " for transaction " .. transactionNumber .. ".");
                ExecuteCommand("SendTransactionNotification", {transactionNumber, Settings.EmailName});
            end
        end
    end
end

function SetSharedServerSupport(connection)
    connection.QueryString = "SELECT Value FROM Customization WHERE CustKey = 'SharedServerSupport' AND NVTGC = 'ILL'";
    local value = connection:ExecuteScalar();

    if value == "Yes" then
        log:Debug("Shared Server Support enabled");
        sharedServerSupport = true;
    else
        log:Debug("Shared Server Support not enabled");
        sharedServerSupport = false;
    end
end

function CacheSystemManagerAddonInterval(connection)
    connection.QueryString = "SELECT Value FROM Customization WHERE CustKey = 'SystemManagerAddonInterval' AND NVTGC = 'ILL'";
    local value = connection:ExecuteScalar();

    if value and value ~= "" then
        log:Debug("Caching SystemManagerAddonInterval customization key. Value: " .. value);
        systemManagerAddonInterval = tonumber(value);
    else
        log:Debug("Valid value not found when attempting to cache the SystemManagerAddonInterval customization key. Using system default of 5.");
        systemManagerAddonInterval = 5;
    end
end

function TraverseError(e)
    if not e.GetType then
        -- Not a .NET type
        return e;
    else
        if not e.Message then
            -- Not a .NET exception
            return e;
        end
    end

    log:Debug(e.Message);

    if e.InnerException then
        return TraverseError(e.InnerException);
    else
        return e.Message;
    end
end

function OnError(err)
    -- To ensure the addon doesn't get stuck in processing if it encounters an error.
    isCurrentlyProcessing = false;
    log:Error(tostring(TraverseError(err)));
end