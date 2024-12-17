const hubspot = require('@hubspot/api-client');
const { queue } = require('async');
const _ = require('lodash');

const { filterNullValuesFromObject, goal } = require('./utils');
const Domain = require('./Domain');

const hubspotClient = new hubspot.Client({ accessToken: '' });
const propertyPrefix = 'hubspot__';
let expirationDate;

const generateLastModifiedDateFilter = (date, nowDate, propertyName = 'hs_lastmodifieddate') => {
  const lastModifiedDateFilter = date ?
    {
      filters: [
        { propertyName, operator: 'GTE', value: `${date.valueOf()}` },
        { propertyName, operator: 'LTE', value: `${nowDate.valueOf()}` }
      ]
    } :
    {};

  return lastModifiedDateFilter;
};

const saveDomain = async domain => {
  // disable this for testing purposes
  return;

  domain.markModified('integrations.hubspot.accounts');
  await domain.save();
};

/**
 * Get access token from HubSpot
 */
const refreshAccessToken = async (domain, hubId, tryCount) => {
  const { HUBSPOT_CID, HUBSPOT_CS } = process.env;
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const { accessToken, refreshToken } = account;

  return hubspotClient.oauth.tokensApi
    .createToken('refresh_token', undefined, undefined, HUBSPOT_CID, HUBSPOT_CS, refreshToken)
    .then(async result => {
      const body = result.body ? result.body : result;

      const newAccessToken = body.accessToken;
      expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());

      hubspotClient.setAccessToken(newAccessToken);
      if (newAccessToken !== accessToken) {
        account.accessToken = newAccessToken;
      }

      return true;
    });
};

/**
 * Get recently modified companies as 100 companies per page
 */
const processCompanies = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.companies);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now);
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'name',
        'domain',
        'country',
        'industry',
        'description',
        'annualrevenue',
        'numberofemployees',
        'hs_lead_status'
      ],
      limit,
      after: offsetObject.after
    };

    let searchResult = {};

    let tryCount = 0;
    while (tryCount <= 4) {
      try {
        searchResult = await hubspotClient.crm.companies.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) throw new Error('Failed to fetch companies for the 4th time. Aborting.');

    const data = searchResult?.results || [];
    offsetObject.after = parseInt(searchResult?.paging?.next?.after);

    console.log('fetch company batch');

    data.forEach(company => {
      if (!company.properties) return;

      const actionTemplate = {
        includeInAnalytics: 0,
        companyProperties: {
          company_id: company.id,
          company_domain: company.properties.domain,
          company_industry: company.properties.industry
        }
      };

      const isCreated = !lastPulledDate || (new Date(company.createdAt) > lastPulledDate);

      q.push({
        actionName: isCreated ? 'Company Created' : 'Company Updated',
        actionDate: new Date(isCreated ? company.createdAt : company.updatedAt) - 2000,
        ...actionTemplate
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.companies = now;
  await saveDomain(domain);

  return true;
};

/**
 * Get recently modified contacts as 100 contacts per page
 */
const processContacts = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.contacts);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, 'lastmodifieddate');
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'firstname',
        'lastname',
        'jobtitle',
        'email',
        'hubspotscore',
        'hs_lead_status',
        'hs_analytics_source',
        'hs_latest_source'
      ],
      limit,
      after: offsetObject.after
    };

    let searchResult = {};

    let tryCount = 0;
    while (tryCount <= 4) {
      try {
        searchResult = await hubspotClient.crm.contacts.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) throw new Error('Failed to fetch contacts for the 4th time. Aborting.');

    const data = searchResult.results || [];

    console.log('fetch contact batch');

    offsetObject.after = parseInt(searchResult.paging?.next?.after);
    const contactIds = data.map(contact => contact.id);

    // contact to company association
    const contactsToAssociate = contactIds;
    const companyAssociationsResults = (await (await hubspotClient.apiRequest({
      method: 'post',
      path: '/crm/v3/associations/CONTACTS/COMPANIES/batch/read',
      body: { inputs: contactsToAssociate.map(contactId => ({ id: contactId })) }
    })).json())?.results || [];

    const companyAssociations = Object.fromEntries(companyAssociationsResults.map(a => {
      if (a.from) {
        contactsToAssociate.splice(contactsToAssociate.indexOf(a.from.id), 1);
        return [a.from.id, a.to[0].id];
      } else return false;
    }).filter(x => x));

    data.forEach(contact => {
      if (!contact.properties || !contact.properties.email) return;

      const companyId = companyAssociations[contact.id];

      const isCreated = new Date(contact.createdAt) > lastPulledDate;

      const userProperties = {
        company_id: companyId,
        contact_name: ((contact.properties.firstname || '') + ' ' + (contact.properties.lastname || '')).trim(),
        contact_title: contact.properties.jobtitle,
        contact_source: contact.properties.hs_analytics_source,
        contact_status: contact.properties.hs_lead_status,
        contact_score: parseInt(contact.properties.hubspotscore) || 0
      };

      const actionTemplate = {
        includeInAnalytics: 0,
        identity: contact.properties.email,
        userProperties: filterNullValuesFromObject(userProperties)
      };

      q.push({
        actionName: isCreated ? 'Contact Created' : 'Contact Updated',
        actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
        ...actionTemplate
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.contacts = now;
  await saveDomain(domain);

  return true;
};

/**
 * Get recently modified meetings as 100 meetings per page
 */
const processMeetings = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.meetings);
  const now = new Date();

  console.log('Starting to process meetings', {
    hubId,
    lastPulledDate: lastPulledDate.toISOString(),
    currentTime: now.toISOString()
  });

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;
  let totalProcessed = 0;
  let totalCreated = 0;
  let totalUpdated = 0;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, 'hs_lastmodifieddate');
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'hs_meeting_title',
        'hs_meeting_body',
        'hs_meeting_start_time',
        'hs_meeting_end_time',
        'hs_meeting_outcome'
      ],
      limit,
      after: offsetObject.after
    };

    console.log('Fetching meetings batch with params:', {
      lastModifiedDate: lastModifiedDate.toISOString(),
      after: offsetObject.after || 'initial'
    });

    let searchResult = {};

    let tryCount = 0;
    while (tryCount <= 4) {
      try {
        searchResult = await hubspotClient.crm.objects.meetings.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        console.log('Error fetching meetings batch:', {
          tryCount,
          error: err.message,
          status: err.status
        });
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) throw new Error('Failed to fetch meetings for the 4th time. Aborting.');

    const data = searchResult?.results || [];
    offsetObject.after = parseInt(searchResult?.paging?.next?.after);

    console.log('Fetched meetings batch:', {
      batchSize: data.length,
      hasNextPage: !!offsetObject.after,
      totalProcessedSoFar: totalProcessed
    });

    // Get meeting IDs for association lookup
    const meetingIds = data.map(meeting => meeting.id);

    // Fetch contact associations for all meetings in the batch
    console.log('Fetching contact associations for meetings:', { meetingCount: meetingIds.length });
    const contactAssociationsResults = (await (await hubspotClient.apiRequest({
      method: 'post',
      path: '/crm/v3/associations/meetings/contacts/batch/read',
      body: { inputs: meetingIds.map(meetingId => ({ id: meetingId })) }
    })).json())?.results || [];

    // Create a map of meeting ID to contact IDs
    const meetingContactAssociations = {};
    contactAssociationsResults.forEach(result => {
      if (result.from && result.to && result.to.length > 0) {
        meetingContactAssociations[result.from.id] = result.to.map(to => to.id);
      }
    });

    // Fetch contact details for all associated contacts
    const allContactIds = [...new Set(Object.values(meetingContactAssociations).flat())];
    console.log('Fetching contact details:', { contactCount: allContactIds.length });
    
    const contactDetailsResults = allContactIds.length > 0 ? await hubspotClient.crm.contacts.batchApi.read({
      inputs: allContactIds.map(id => ({ id })),
      properties: ['email']
    }) : { results: [] };

    // Create a map of contact ID to email
    const contactEmailMap = {};
    contactDetailsResults.results.forEach(contact => {
      if (contact.properties?.email) {
        contactEmailMap[contact.id] = contact.properties.email;
      }
    });

    data.forEach(meeting => {
      if (!meeting.properties) {
        console.log('Skipping meeting due to missing properties:', { meetingId: meeting.id });
        return;
      }

      console.log('Processing meeting:', {
        meetingId: meeting.id,
        title: meeting.properties.hs_meeting_title,
        startTime: meeting.properties.hs_meeting_start_time,
        endTime: meeting.properties.hs_meeting_end_time,
        outcome: meeting.properties.hs_meeting_outcome
      });

      const associatedContactIds = meetingContactAssociations[meeting.id] || [];
      const contactEmails = associatedContactIds
        .map(contactId => contactEmailMap[contactId])
        .filter(email => email); // Remove undefined emails

      if (contactEmails.length === 0) {
        console.log('No associated contacts found for meeting:', { meetingId: meeting.id });
      } else {
        console.log('Found associated contacts:', {
          meetingId: meeting.id,
          meetingTitle: meeting.properties.hs_meeting_title,
          contactCount: contactEmails.length,
          contacts: contactEmails
        });
      }

      // Create an action for each contact that attended the meeting
      contactEmails.forEach(contactEmail => {
        console.log('Creating action for contact:', {
          meetingId: meeting.id,
          meetingTitle: meeting.properties.hs_meeting_title,
          contactEmail,
          actionType: !lastPulledDate || (new Date(meeting.createdAt) > lastPulledDate) ? 'Meeting Created' : 'Meeting Updated'
        });

        const actionTemplate = {
          includeInAnalytics: 0,
          identity: contactEmail,
          meetingProperties: {
            meeting_id: meeting.id,
            meeting_title: meeting.properties.hs_meeting_title,
            meeting_start_time: meeting.properties.hs_meeting_start_time,
            meeting_end_time: meeting.properties.hs_meeting_end_time,
            meeting_outcome: meeting.properties.hs_meeting_outcome
          }
        };

        const isCreated = !lastPulledDate || (new Date(meeting.createdAt) > lastPulledDate);
        isCreated ? totalCreated++ : totalUpdated++;

        q.push({
          actionName: isCreated ? 'Meeting Created' : 'Meeting Updated',
          actionDate: new Date(isCreated ? meeting.createdAt : meeting.updatedAt),
          ...actionTemplate
        });
      });
    });

    totalProcessed += data.length;

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      console.log('Reached 9900 record limit, resetting pagination and updating lastModifiedDate');
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  console.log('Finished processing meetings:', {
    hubId,
    totalProcessed,
    totalCreated,
    totalUpdated,
    startTime: lastPulledDate.toISOString(),
    endTime: now.toISOString()
  });

  account.lastPulledDates.meetings = now;
  await saveDomain(domain);

  return true;
};

const createQueue = (domain, actions) => queue(async (action, callback) => {
  actions.push(action);

  if (actions.length > 2000) {
    console.log('inserting actions to database', { apiKey: domain.apiKey, count: actions.length });

    const copyOfActions = _.cloneDeep(actions);
    actions.splice(0, actions.length);

    goal(copyOfActions);
  }

  callback();
}, 100000000);

const drainQueue = async (domain, actions, q) => {
  if (q.length() > 0) await q.drain();

  if (actions.length > 0) {
    goal(actions)
  }

  return true;
};

const pullDataFromHubspot = async () => {
  console.log('start pulling data from HubSpot');

  const domain = await Domain.findOne({});

  for (const account of domain.integrations.hubspot.accounts) {
    console.log('start processing account');

    try {
      await refreshAccessToken(domain, account.hubId);
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'refreshAccessToken' } });
    }

    const actions = [];
    const q = createQueue(domain, actions);

    try {
      await processContacts(domain, account.hubId, q);
      console.log('process contacts');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processContacts', hubId: account.hubId } });
    }

    try {
      await processCompanies(domain, account.hubId, q);
      console.log('process companies');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processCompanies', hubId: account.hubId } });
    }

    try {
      await processMeetings(domain, account.hubId, q);
      console.log('process meetings');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processMeetings', hubId: account.hubId } });
    }

    try {
      await drainQueue(domain, actions, q);
      console.log('drain queue');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'drainQueue', hubId: account.hubId } });
    }

    await saveDomain(domain);

    console.log('finish processing account');
  }

  process.exit();
};

module.exports = pullDataFromHubspot;
